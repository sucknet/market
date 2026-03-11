const fs = require('node:fs/promises');
const path = require('node:path');

const bs58 = require('bs58');
const pako = require('pako');
const { BorshAccountsCoder } = require('@coral-xyz/anchor');
const {
  Connection,
  PublicKey,
} = require('@solana/web3.js');
const {
  getMetadataAccountDataSerializer,
} = require('@metaplex-foundation/mpl-token-metadata');

const RPC_URL = process.env.FOGO_RPC_URL || 'https://mainnet.fogo.io';
const MARKETPLACE_PROGRAM_ID = new PublicKey(
  process.env.MARKETPLACE_PROGRAM_ID ||
    'orcJCdDGwunzjWNJqwYJU2VLceao85qwkeKNf9Yos6T'
);
const TOKEN_METADATA_PROGRAM_ID = new PublicKey(
  'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
);
const COLLECTION_FILTER = process.env.COLLECTION_FILTER || null;
const DEFAULT_FOGOFISHING_COLLECTION =
  '5scD29QSn94hLwL3GtjyReTvU1jPTKY5aF6ur6KwGx4o';
const EFFECTIVE_COLLECTION_FILTER = COLLECTION_FILTER || DEFAULT_FOGOFISHING_COLLECTION;
const LISTING_LIMIT = Number(process.env.LISTING_LIMIT || '0');
const FETCH_BATCH_SIZE = Number(process.env.FETCH_BATCH_SIZE || '100');
const OUTPUT_FILE =
  process.env.OUTPUT_FILE ||
  path.join(process.cwd(), 'output', 'active-listings.json');

function trimNulls(value) {
  return typeof value === 'string' ? value.replace(/\0/g, '').trim() : value;
}

function bnToString(value) {
  if (value && typeof value.toString === 'function') {
    return value.toString();
  }
  return String(value);
}

function parseStatus(status) {
  if (!status || typeof status !== 'object') {
    return { status: 'Unknown', buyer: null };
  }

  if (status.Active !== undefined) {
    return { status: 'Active', buyer: null };
  }
  if (status.Unlisted !== undefined) {
    return { status: 'Unlisted', buyer: null };
  }
  if (status.Sold !== undefined) {
    const buyer = status.Sold?.buyer?.toBase58?.() || null;
    return { status: 'Sold', buyer };
  }

  return { status: 'Unknown', buyer: null };
}

async function fetchJson(url) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10_000);
  try {
    const res = await fetch(url, { signal: controller.signal });
    if (!res.ok) {
      return null;
    }
    return await res.json();
  } catch {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}

async function getOnchainIdl(connection, programId) {
  const base = PublicKey.findProgramAddressSync([], programId)[0];
  const idlAddress = await PublicKey.createWithSeed(base, 'anchor:idl', programId);
  const account = await connection.getAccountInfo(idlAddress, 'finalized');

  if (!account) {
    throw new Error(`IDL account not found: ${idlAddress.toBase58()}`);
  }

  const body = Buffer.from(account.data).slice(8);
  const idlDataLength = body.readUInt32LE(32);
  const compressed = body.slice(36, 36 + idlDataLength);
  const idlJson = Buffer.from(pako.inflate(compressed)).toString('utf8');

  return JSON.parse(idlJson);
}

async function mapConcurrent(items, concurrency, fn) {
  const output = new Array(items.length);
  let cursor = 0;

  async function worker() {
    while (true) {
      const index = cursor;
      cursor += 1;
      if (index >= items.length) {
        return;
      }
      output[index] = await fn(items[index], index);
    }
  }

  await Promise.all(
    Array.from({ length: Math.max(1, Math.min(concurrency, items.length)) }, () => worker())
  );
  return output;
}

function chunk(items, size) {
  const chunks = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

async function resolveMetadataForMint(connection, mint) {
  try {
    const mintPk = new PublicKey(mint);
    const metadataPda = PublicKey.findProgramAddressSync(
      [
        Buffer.from('metadata'),
        TOKEN_METADATA_PROGRAM_ID.toBuffer(),
        mintPk.toBuffer(),
      ],
      TOKEN_METADATA_PROGRAM_ID
    )[0];

    const metadataAccount = await connection.getAccountInfo(metadataPda, 'confirmed');
    if (!metadataAccount) {
      return {
        mint,
        metadataPda: metadataPda.toBase58(),
        name: null,
        symbol: null,
        metadataUri: null,
        image: null,
      };
    }

    const serializer = getMetadataAccountDataSerializer();
    const [metadata] = serializer.deserialize(metadataAccount.data);

    const metadataUri = trimNulls(metadata.uri);
    const offchain = metadataUri ? await fetchJson(metadataUri) : null;
    const image =
      offchain?.image ||
      offchain?.properties?.image ||
      offchain?.properties?.files?.find((file) => {
        const type = String(file?.type || '').toLowerCase();
        return type.startsWith('image/');
      })?.uri ||
      null;

    return {
      mint,
      metadataPda: metadataPda.toBase58(),
      name: trimNulls(metadata.name),
      symbol: trimNulls(metadata.symbol),
      metadataUri,
      image,
    };
  } catch {
    return {
      mint,
      metadataPda: null,
      name: null,
      symbol: null,
      metadataUri: null,
      image: null,
    };
  }
}

async function fetchActiveListings(options = {}) {
  const listingLimit = Number.isFinite(options.listingLimit)
    ? Number(options.listingLimit)
    : LISTING_LIMIT;
  const effectiveCollectionFilter =
    options.collectionFilter !== undefined
      ? options.collectionFilter
      : EFFECTIVE_COLLECTION_FILTER;

  const connection = new Connection(RPC_URL, 'confirmed');
  console.log(`RPC: ${RPC_URL}`);
  console.log(`Program: ${MARKETPLACE_PROGRAM_ID.toBase58()}`);
  console.log(`Collection filter: ${effectiveCollectionFilter}`);
  const idl = await getOnchainIdl(connection, MARKETPLACE_PROGRAM_ID);

  const listingAccountDef = (idl.accounts || []).find((a) => a.name === 'Listing');
  if (!listingAccountDef) {
    throw new Error('Listing account not found in IDL');
  }

  const listingDiscriminator = Buffer.from(listingAccountDef.discriminator);
  const coder = new BorshAccountsCoder(idl);

  const listingPointers = await connection.getProgramAccounts(MARKETPLACE_PROGRAM_ID, {
    filters: [{ memcmp: { offset: 0, bytes: bs58.encode(listingDiscriminator) } }],
    dataSlice: { offset: 0, length: 0 },
    encoding: 'base64',
    commitment: 'confirmed',
  });

  const listingPubkeys = listingPointers.map((row) => row.pubkey);
  const pubkeyBatches = chunk(listingPubkeys, FETCH_BATCH_SIZE);

  const listingAccounts = [];
  for (const pubkeys of pubkeyBatches) {
    const infos = await connection.getMultipleAccountsInfo(pubkeys, 'confirmed');
    for (let i = 0; i < pubkeys.length; i += 1) {
      if (!infos[i]) {
        continue;
      }
      listingAccounts.push({
        pubkey: pubkeys[i],
        account: infos[i],
      });
    }
  }

  const decodedListings = listingAccounts
    .map(({ pubkey, account }) => {
      const listing = coder.decode('Listing', account.data);
      const status = parseStatus(listing.status);
      return {
        listingAddress: pubkey.toBase58(),
        globalIndex: bnToString(listing.global_index),
        sellerIndex: bnToString(listing.seller_index),
        assetIndex: bnToString(listing.asset_index),
        seller: listing.seller.toBase58(),
        assetMint: listing.asset.toBase58(),
        currencyMint: listing.currency_mint.toBase58(),
        priceAtomic: bnToString(listing.price),
        priceUi: Number(bnToString(listing.price)) / 1e9,
        status: status.status,
        buyer: status.buyer,
        createdAt: Number(bnToString(listing.created_at)),
        closedAt: Number(bnToString(listing.closed_at)),
        collection: listing.collection.toBase58(),
      };
    })
    .filter((listing) =>
      effectiveCollectionFilter ? listing.collection === effectiveCollectionFilter : true
    )
    .filter((listing) => listing.status === 'Active')
    .sort((a, b) => b.createdAt - a.createdAt);

  const limitedListings = listingLimit > 0
    ? decodedListings.slice(0, listingLimit)
    : decodedListings;

  console.log(`Active listings selected: ${limitedListings.length}`);

  const uniqueMints = [...new Set(limitedListings.map((listing) => listing.assetMint))];
  const metadataRows = await mapConcurrent(uniqueMints, 12, async (mint) =>
    resolveMetadataForMint(connection, mint)
  );
  console.log(`Metadata resolved for unique mints: ${metadataRows.length}`);
  const metadataByMint = new Map(metadataRows.map((row) => [row.mint, row]));

  const listingsWithImage = limitedListings.map((listing) => {
    const metadata = metadataByMint.get(listing.assetMint) || null;
    return {
      ...listing,
      name: metadata?.name || null,
      symbol: metadata?.symbol || null,
      metadataUri: metadata?.metadataUri || null,
      image: metadata?.image || null,
      metadataPda: metadata?.metadataPda || null,
    };
  });

  return {
    rpc: RPC_URL,
    programId: MARKETPLACE_PROGRAM_ID.toBase58(),
    generatedAt: new Date().toISOString(),
    totalActiveListings: listingsWithImage.length,
    listings: listingsWithImage,
  };
}

async function writePayloadToFile(payload, outputFile = OUTPUT_FILE) {
  await fs.mkdir(path.dirname(outputFile), { recursive: true });
  await fs.writeFile(outputFile, JSON.stringify(payload, null, 2), 'utf8');
}

async function main() {
  const payload = await fetchActiveListings();
  await writePayloadToFile(payload);

  console.log(`Saved ${payload.totalActiveListings} active listings to ${OUTPUT_FILE}`);
  const withImage = payload.listings.filter((item) => !!item.image).length;
  console.log(`Listings with image URL: ${withImage}`);
}

if (require.main === module) {
  main().catch((error) => {
    console.error(error);
    process.exit(1);
  });
}

module.exports = {
  fetchActiveListings,
  writePayloadToFile,
};
