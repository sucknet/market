const http = require('node:http');
const fs = require('node:fs/promises');
const path = require('node:path');

const bs58 = require('bs58');
const pako = require('pako');
const { BorshAccountsCoder } = require('@coral-xyz/anchor');
const {
  Connection,
  PublicKey,
  Transaction,
  TransactionInstruction,
  SystemProgram,
} = require('@solana/web3.js');
const { getMetadataAccountDataSerializer } = require('@metaplex-foundation/mpl-token-metadata');

const HOST = process.env.HOST || '0.0.0.0';
const PORT = Number(process.env.PORT || '8787');
const REFRESH_MS = Number(process.env.REFRESH_MS || '5000');
const BACKFILL_BATCH = Number(process.env.BACKFILL_BATCH || '100');
const HEAD_BATCH = Number(process.env.HEAD_BATCH || '100');
const ENRICH_PER_TICK = Number(process.env.ENRICH_PER_TICK || '24');
const TX_CONCURRENCY = Number(process.env.TX_CONCURRENCY || '10');
const BACKFILL_STEPS_PER_TICK = Number(process.env.BACKFILL_STEPS_PER_TICK || '4');
const BACKFILL_TIME_BUDGET_MS = Number(process.env.BACKFILL_TIME_BUDGET_MS || '3500');
const DEFAULT_PAGE_SIZE = Number(process.env.PAGE_SIZE || '60');
const PUBLIC_DIR = path.join(process.cwd(), 'public');

const RPC_URL = process.env.FOGO_RPC_URL || 'https://mainnet.fogo.io';
const MARKETPLACE_PROGRAM_ID = new PublicKey(
  process.env.MARKETPLACE_PROGRAM_ID ||
    'orcJCdDGwunzjWNJqwYJU2VLceao85qwkeKNf9Yos6T'
);
const TOKEN_METADATA_PROGRAM_ID = new PublicKey(
  'metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s'
);
const TOKEN_PROGRAM_ID = new PublicKey('TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA');
const ASSOCIATED_TOKEN_PROGRAM_ID = new PublicKey('ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL');
const MPL_CORE_PROGRAM_ID = new PublicKey('CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d');
const SYSVAR_INSTRUCTIONS_PUBKEY = new PublicKey('Sysvar1nstructions1111111111111111111111111');
const COLLECTION_FILTER =
  process.env.COLLECTION_FILTER ||
  '5scD29QSn94hLwL3GtjyReTvU1jPTKY5aF6ur6KwGx4o';

const connection = new Connection(RPC_URL, 'confirmed');

const state = {
  bootstrapped: false,
  loading: false,
  lastOkAt: null,
  lastError: null,
  idl: null,
  instructionMap: null,
  newestSigSeen: null,
  backfillBefore: null,
  backfillDone: false,
  processedSignatures: new Set(),
  listingsByAddress: new Map(),
  metadataByMint: new Map(),
  accountDecodeCache: new Map(),
  txConcurrency: Math.max(2, TX_CONCURRENCY),
  backfillStepsPerTick: Math.max(1, BACKFILL_STEPS_PER_TICK),
  successStreak: 0,
};

const IMAGE_RETRY_MS = 30_000;

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

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function trimNulls(value) {
  return typeof value === 'string' ? value.replace(/\0/g, '').trim() : value;
}

function json(res, statusCode, body) {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store',
  });
  res.end(JSON.stringify(body));
}

function badRequest(res, message) {
  return json(res, 400, { ok: false, error: message });
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  if (!chunks.length) {
    return {};
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

function asBigInt(value) {
  if (typeof value === 'bigint') {
    return value;
  }
  if (typeof value === 'number') {
    return BigInt(value);
  }
  if (value && typeof value.toString === 'function') {
    return BigInt(value.toString());
  }
  return 0n;
}

function u64Le(value) {
  const out = Buffer.alloc(8);
  out.writeBigUInt64LE(asBigInt(value));
  return out;
}

function pda(seeds) {
  return PublicKey.findProgramAddressSync(seeds, MARKETPLACE_PROGRAM_ID)[0];
}

function getAta(owner, mint) {
  return PublicKey.findProgramAddressSync(
    [owner.toBuffer(), TOKEN_PROGRAM_ID.toBuffer(), mint.toBuffer()],
    ASSOCIATED_TOKEN_PROGRAM_ID
  )[0];
}

function createAtaIdempotentInstruction({ payer, ata, owner, mint }) {
  return new TransactionInstruction({
    programId: ASSOCIATED_TOKEN_PROGRAM_ID,
    keys: [
      { pubkey: payer, isSigner: true, isWritable: true },
      { pubkey: ata, isSigner: false, isWritable: true },
      { pubkey: owner, isSigner: false, isWritable: false },
      { pubkey: mint, isSigner: false, isWritable: false },
      { pubkey: SystemProgram.programId, isSigner: false, isWritable: false },
      { pubkey: TOKEN_PROGRAM_ID, isSigner: false, isWritable: false },
    ],
    // Associated token account instruction enum: 1 = CreateIdempotent.
    data: Buffer.from([1]),
  });
}

function getConfigPda() {
  return pda([Buffer.from('config')]);
}

function getAllowedCollectionPda(collectionPk) {
  return pda([Buffer.from('collection'), collectionPk.toBuffer()]);
}

function getAllowedCurrencyPda(currencyMintPk) {
  return pda([Buffer.from('allowed_currency'), currencyMintPk.toBuffer()]);
}

function getListingVersionCounterPda(assetPk) {
  return pda([Buffer.from('listing'), Buffer.from('version_counter'), assetPk.toBuffer()]);
}

function getListingPda(assetPk, versionCount) {
  return pda([Buffer.from('listing'), assetPk.toBuffer(), u64Le(versionCount)]);
}

function getGlobalStatsPda() {
  return pda([Buffer.from('global_stats')]);
}

function getUserStatsPda(userPk) {
  return pda([Buffer.from('user_stats'), userPk.toBuffer()]);
}

function getGlobalListingPointerPda(totalListings) {
  return pda([Buffer.from('listing'), Buffer.from('global'), u64Le(totalListings)]);
}

function getUserListingPointerPda(userPk, totalListings) {
  return pda([Buffer.from('listing'), Buffer.from('user'), userPk.toBuffer(), u64Le(totalListings)]);
}

function getCollectionListingPointerPda(collectionPk, totalListings) {
  return pda([
    Buffer.from('listing'),
    Buffer.from('collection'),
    collectionPk.toBuffer(),
    u64Le(totalListings),
  ]);
}

function getGlobalActivityPda(totalActivities) {
  return pda([Buffer.from('activity'), Buffer.from('global'), u64Le(totalActivities)]);
}

function getUserActivityPda(userPk, totalActivities) {
  return pda([Buffer.from('activity'), Buffer.from('user'), userPk.toBuffer(), u64Le(totalActivities)]);
}

function getCollectionActivityPda(collectionPk, totalActivities) {
  return pda([
    Buffer.from('activity'),
    Buffer.from('collection'),
    collectionPk.toBuffer(),
    u64Le(totalActivities),
  ]);
}

async function decodeProgramAccount(accountName, pubkey) {
  const info = await connection.getAccountInfo(pubkey, 'confirmed');
  if (!info) {
    return null;
  }

  const coder = new BorshAccountsCoder(state.idl);
  return coder.decode(accountName, info.data);
}

function buildInstructionByIdl(ixName, accountsByName, args = {}, remainingAccounts = []) {
  const ixDef = (state.idl.instructions || []).find((x) => x.name === ixName);
  if (!ixDef) {
    throw new Error(`Instruction not found in IDL: ${ixName}`);
  }

  const keys = (ixDef.accounts || []).map((acc) => {
    const value = accountsByName[acc.name];
    if (!value) {
      throw new Error(`Missing account for ${ixName}.${acc.name}`);
    }
    return {
      pubkey: value,
      isSigner: !!acc.signer,
      isWritable: !!acc.writable,
    };
  });

  for (const extra of remainingAccounts) {
    if (!extra?.pubkey) {
      continue;
    }
    keys.push({
      pubkey: extra.pubkey,
      isSigner: !!extra.isSigner,
      isWritable: !!extra.isWritable,
    });
  }

  let argBytes = Buffer.alloc(0);
  if (ixName === 'list') {
    argBytes = u64Le(args.priceAtomic || 0n);
  }

  const data = Buffer.concat([Buffer.from(ixDef.discriminator || []), argBytes]);
  return new TransactionInstruction({
    programId: MARKETPLACE_PROGRAM_ID,
    keys,
    data,
  });
}

async function buildBuyInstruction(params) {
  const wallet = new PublicKey(params.wallet);
  const listingAddress = String(params.listingAddress || '');
  const listing = await decodeListingAccount(listingAddress);
  if (!listing || listing.status !== 'Active') {
    throw new Error('Active listing not found for buy');
  }

  const sellerPk = new PublicKey(listing.seller);
  const assetPk = new PublicKey(listing.assetMint);
  const collectionPk = new PublicKey(listing.collection);
  const currencyMintPk = new PublicKey(listing.currencyMint);
  const listingPk = new PublicKey(listing.listingAddress);

  const configPk = getConfigPda();
  const config = await decodeProgramAccount('Config', configPk);
  if (!config?.treasury) {
    throw new Error('Config account unavailable');
  }

  const globalStatsPk = getGlobalStatsPda();
  const globalStats = await decodeProgramAccount('GlobalStats', globalStatsPk);
  const allowedCollectionPk = getAllowedCollectionPda(collectionPk);
  const allowedCollection = await decodeProgramAccount('AllowedCollection', allowedCollectionPk);
  const buyerStatsPk = getUserStatsPda(wallet);
  const sellerStatsPk = getUserStatsPda(sellerPk);
  const buyerStats = await decodeProgramAccount('UserStats', buyerStatsPk);
  const sellerStats = await decodeProgramAccount('UserStats', sellerStatsPk);

  const accounts = {
    payer: wallet,
    seller: sellerPk,
    asset: assetPk,
    collection: collectionPk,
    allowed_collection: allowedCollectionPk,
    config: configPk,
    currency_mint: currencyMintPk,
    allowed_currency: getAllowedCurrencyPda(currencyMintPk),
    fogo_session: wallet,
    buyer: wallet,
    buyer_token_account: getAta(wallet, currencyMintPk),
    seller_token_account: getAta(sellerPk, currencyMintPk),
    treasury_token_account: getAta(new PublicKey(config.treasury), currencyMintPk),
    listing_version_counter: getListingVersionCounterPda(assetPk),
    listing: listingPk,
    global_stats: globalStatsPk,
    buyer_stats: buyerStatsPk,
    seller_stats: sellerStatsPk,
    global_activity_buy: getGlobalActivityPda(asBigInt(globalStats?.total_activities || 0n)),
    buyer_activity: getUserActivityPda(wallet, asBigInt(buyerStats?.total_activities || 0n)),
    collection_activity: getCollectionActivityPda(
      collectionPk,
      asBigInt(allowedCollection?.total_activities || 0n)
    ),
    global_activity_sell: getGlobalActivityPda(asBigInt(globalStats?.total_activities || 0n) + 1n),
    seller_activity: getUserActivityPda(sellerPk, asBigInt(sellerStats?.total_activities || 0n)),
    system_program: SystemProgram.programId,
  };

  return buildInstructionByIdl('buy', accounts);
}

async function buildUnlistInstruction(params) {
  const wallet = new PublicKey(params.wallet);
  const listingAddress = String(params.listingAddress || '');
  const listing = await decodeListingAccount(listingAddress);
  if (!listing || listing.status !== 'Active') {
    throw new Error('Active listing not found for delist');
  }

  if (listing.seller !== wallet.toBase58()) {
    throw new Error('Only listing owner can delist');
  }

  const sellerPk = wallet;
  const assetPk = new PublicKey(listing.assetMint);
  const collectionPk = new PublicKey(listing.collection);
  const currencyMintPk = new PublicKey(listing.currencyMint);
  const listingPk = new PublicKey(listing.listingAddress);

  const configPk = getConfigPda();
  const config = await decodeProgramAccount('Config', configPk);
  if (!config?.treasury) {
    throw new Error('Config account unavailable');
  }

  const globalStatsPk = getGlobalStatsPda();
  const globalStats = await decodeProgramAccount('GlobalStats', globalStatsPk);
  const allowedCollectionPk = getAllowedCollectionPda(collectionPk);
  const allowedCollection = await decodeProgramAccount('AllowedCollection', allowedCollectionPk);
  const userStatsPk = getUserStatsPda(sellerPk);
  const userStats = await decodeProgramAccount('UserStats', userStatsPk);

  const accounts = {
    payer: wallet,
    fogo_session: wallet,
    asset: assetPk,
    collection: collectionPk,
    allowed_collection: allowedCollectionPk,
    config: configPk,
    allowed_currency: getAllowedCurrencyPda(currencyMintPk),
    currency_mint: currencyMintPk,
    seller: sellerPk,
    seller_token_account: getAta(sellerPk, currencyMintPk),
    treasury_token_account: getAta(new PublicKey(config.treasury), currencyMintPk),
    listing_version_counter: getListingVersionCounterPda(assetPk),
    listing: listingPk,
    global_stats: globalStatsPk,
    user_stats: userStatsPk,
    global_activity: getGlobalActivityPda(asBigInt(globalStats?.total_activities || 0n)),
    user_activity: getUserActivityPda(sellerPk, asBigInt(userStats?.total_activities || 0n)),
    collection_activity_pointer: getCollectionActivityPda(
      collectionPk,
      asBigInt(allowedCollection?.total_activities || 0n)
    ),
    mpl_core_program: MPL_CORE_PROGRAM_ID,
    system_program: SystemProgram.programId,
    token_program: TOKEN_PROGRAM_ID,
    associated_token_program: ASSOCIATED_TOKEN_PROGRAM_ID,
    token_metadata_program: TOKEN_METADATA_PROGRAM_ID,
    sysvar_instructions: SYSVAR_INSTRUCTIONS_PUBKEY,
  };

  const sellerAssetTokenAccount = getAta(sellerPk, assetPk);
  const tokenMetadataAcc = PublicKey.findProgramAddressSync(
    [
      Buffer.from('metadata'),
      TOKEN_METADATA_PROGRAM_ID.toBuffer(),
      assetPk.toBuffer(),
    ],
    TOKEN_METADATA_PROGRAM_ID
  )[0];
  const editionAccount = PublicKey.findProgramAddressSync(
    [
      Buffer.from('metadata'),
      TOKEN_METADATA_PROGRAM_ID.toBuffer(),
      assetPk.toBuffer(),
      Buffer.from('edition'),
    ],
    TOKEN_METADATA_PROGRAM_ID
  )[0];

  const remaining = [
    { pubkey: sellerAssetTokenAccount, isWritable: true, isSigner: false },
    { pubkey: tokenMetadataAcc, isWritable: true, isSigner: false },
    { pubkey: editionAccount, isWritable: false, isSigner: false },
  ];

  return buildInstructionByIdl('unlist', accounts, {}, remaining);
}

async function buildListInstruction(params) {
  const wallet = new PublicKey(params.wallet);
  const assetPk = new PublicKey(String(params.assetMint || ''));
  const collectionPk = new PublicKey(String(params.collection || COLLECTION_FILTER));
  const currencyMintPk = new PublicKey(String(params.currencyMint || 'CzLSujWBLyHJS2pqL5qHj79hBf7xJqBfJwbKXQ2e9Akx'));
  const priceUi = Number(params.priceUi || 0);

  if (!Number.isFinite(priceUi) || priceUi <= 0) {
    throw new Error('priceUi must be greater than 0');
  }

  const priceAtomic = BigInt(Math.round(priceUi * 1e9));

  const configPk = getConfigPda();
  const config = await decodeProgramAccount('Config', configPk);
  if (!config?.treasury) {
    throw new Error('Config account unavailable');
  }

  const allowedCollectionPk = getAllowedCollectionPda(collectionPk);
  const allowedCollection = await decodeProgramAccount('AllowedCollection', allowedCollectionPk);
  const listingVersionCounterPk = getListingVersionCounterPda(assetPk);
  const listingVersionCounter = await decodeProgramAccount(
    'AssetListingVersionCounter',
    listingVersionCounterPk
  );
  const versionCount = asBigInt(listingVersionCounter?.version_count || 0n);
  const listingPk = getListingPda(assetPk, versionCount);

  const globalStatsPk = getGlobalStatsPda();
  const globalStats = await decodeProgramAccount('GlobalStats', globalStatsPk);
  const userStatsPk = getUserStatsPda(wallet);
  const userStats = await decodeProgramAccount('UserStats', userStatsPk);

  const sellerTokenAccountPk = getAta(wallet, currencyMintPk);
  const treasuryTokenAccountPk = getAta(new PublicKey(config.treasury), currencyMintPk);

  const [sellerTokenInfo, treasuryTokenInfo] = await connection.getMultipleAccountsInfo(
    [sellerTokenAccountPk, treasuryTokenAccountPk],
    'confirmed'
  );

  const sellerAssetTokenAccountPk = getAta(wallet, assetPk);
  const tokenMetadataAcc = PublicKey.findProgramAddressSync(
    [
      Buffer.from('metadata'),
      TOKEN_METADATA_PROGRAM_ID.toBuffer(),
      assetPk.toBuffer(),
    ],
    TOKEN_METADATA_PROGRAM_ID
  )[0];
  const editionAccount = PublicKey.findProgramAddressSync(
    [
      Buffer.from('metadata'),
      TOKEN_METADATA_PROGRAM_ID.toBuffer(),
      assetPk.toBuffer(),
      Buffer.from('edition'),
    ],
    TOKEN_METADATA_PROGRAM_ID
  )[0];

  const preInstructions = [];
  if (!sellerTokenInfo) {
    preInstructions.push(
      createAtaIdempotentInstruction({
        payer: wallet,
        ata: sellerTokenAccountPk,
        owner: wallet,
        mint: currencyMintPk,
      })
    );
  }
  if (!treasuryTokenInfo) {
    preInstructions.push(
      createAtaIdempotentInstruction({
        payer: wallet,
        ata: treasuryTokenAccountPk,
        owner: new PublicKey(config.treasury),
        mint: currencyMintPk,
      })
    );
  }

  const accounts = {
    payer: wallet,
    fogo_session: wallet,
    asset: assetPk,
    collection: collectionPk,
    allowed_collection: allowedCollectionPk,
    config: configPk,
    allowed_currency: getAllowedCurrencyPda(currencyMintPk),
    currency_mint: currencyMintPk,
    seller: wallet,
    seller_token_account: sellerTokenAccountPk,
    treasury_token_account: treasuryTokenAccountPk,
    listing_version_counter: listingVersionCounterPk,
    listing: listingPk,
    global_stats: globalStatsPk,
    global_listing_pointer: getGlobalListingPointerPda(asBigInt(globalStats?.total_listings || 0n)),
    user_stats: userStatsPk,
    user_listing_pointer: getUserListingPointerPda(wallet, asBigInt(userStats?.total_listings || 0n)),
    collection_listing_pointer: getCollectionListingPointerPda(
      collectionPk,
      asBigInt(allowedCollection?.total_listings || 0n)
    ),
    global_activity: getGlobalActivityPda(asBigInt(globalStats?.total_activities || 0n)),
    user_activity: getUserActivityPda(wallet, asBigInt(userStats?.total_activities || 0n)),
    collection_activity_pointer: getCollectionActivityPda(
      collectionPk,
      asBigInt(allowedCollection?.total_activities || 0n)
    ),
    system_program: SystemProgram.programId,
  };

  const remaining = [
    { pubkey: SYSVAR_INSTRUCTIONS_PUBKEY, isWritable: false, isSigner: false },
    { pubkey: TOKEN_PROGRAM_ID, isWritable: false, isSigner: false },
    { pubkey: MPL_CORE_PROGRAM_ID, isWritable: false, isSigner: false },
    { pubkey: TOKEN_METADATA_PROGRAM_ID, isWritable: false, isSigner: false },
    { pubkey: sellerAssetTokenAccountPk, isWritable: true, isSigner: false },
    { pubkey: tokenMetadataAcc, isWritable: true, isSigner: false },
    { pubkey: editionAccount, isWritable: false, isSigner: false },
  ];

  return {
    preInstructions,
    instruction: buildInstructionByIdl('list', accounts, { priceAtomic }, remaining),
  };
}

async function buildUnsignedTxBase64({ wallet, action, payload }) {
  if (!state.bootstrapped || !state.idl) {
    state.idl = await getOnchainIdl();
    state.instructionMap = toInstructionMap(state.idl);
    state.bootstrapped = true;
  }

  // Never reuse decoded PDA counters between tx builds; these counters mutate every list/buy/unlist.
  state.accountDecodeCache.clear();

  const instructions = [];
  if (action === 'buy') {
    instructions.push(await buildBuyInstruction({ ...payload, wallet }));
  } else if (action === 'delist') {
    instructions.push(await buildUnlistInstruction({ ...payload, wallet }));
  } else if (action === 'list') {
    const built = await buildListInstruction({ ...payload, wallet });
    if (Array.isArray(built.preInstructions) && built.preInstructions.length) {
      instructions.push(...built.preInstructions);
    }
    instructions.push(built.instruction);
  } else {
    throw new Error(`Unsupported action: ${action}`);
  }

  const walletPk = new PublicKey(wallet);
  const latest = await connection.getLatestBlockhash('confirmed');
  const tx = new Transaction({
    feePayer: walletPk,
    recentBlockhash: latest.blockhash,
  });
  for (const ix of instructions) {
    tx.add(ix);
  }

  return {
    txBase64: tx.serialize({ requireAllSignatures: false, verifySignatures: false }).toString('base64'),
    lastValidBlockHeight: latest.lastValidBlockHeight,
  };
}

async function serveFile(res, filePath) {
  try {
    const content = await fs.readFile(filePath);
    const ext = path.extname(filePath).toLowerCase();
    const contentType =
      ext === '.html'
        ? 'text/html; charset=utf-8'
        : ext === '.js'
          ? 'application/javascript; charset=utf-8'
          : ext === '.css'
            ? 'text/css; charset=utf-8'
            : 'application/octet-stream';

    res.writeHead(200, { 'Content-Type': contentType });
    res.end(content);
  } catch {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Not found');
  }
}

function toInstructionMap(idl) {
  const map = new Map();
  for (const ix of idl.instructions || []) {
    const disc = Buffer.from(ix.discriminator || []);
    map.set(bs58.encode(disc), {
      name: ix.name,
      accountIndexByName: Object.fromEntries(
        (ix.accounts || []).map((a, i) => [a.name, i])
      ),
    });
  }
  return map;
}

async function getOnchainIdl() {
  const base = PublicKey.findProgramAddressSync([], MARKETPLACE_PROGRAM_ID)[0];
  const idlAddress = await PublicKey.createWithSeed(
    base,
    'anchor:idl',
    MARKETPLACE_PROGRAM_ID
  );
  const account = await connection.getAccountInfo(idlAddress, 'finalized');
  if (!account) {
    throw new Error(`IDL account not found: ${idlAddress.toBase58()}`);
  }

  const body = Buffer.from(account.data).slice(8);
  const idlDataLength = body.readUInt32LE(32);
  const compressed = body.slice(36, 36 + idlDataLength);
  return JSON.parse(Buffer.from(pako.inflate(compressed)).toString('utf8'));
}

async function fetchMetadataJsonWithRetry(metadataUri, attempts = 3) {
  for (let i = 0; i < attempts; i += 1) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 8000);
      const res = await fetch(metadataUri, { signal: controller.signal });
      clearTimeout(timeout);
      if (res.ok) {
        return await res.json();
      }
    } catch {
      // retry
    }
  }
  return null;
}

async function fetchMetadataForMint(mint, options = {}) {
  const force = !!options.force;
  const cached = state.metadataByMint.get(mint);
  if (cached?.image) {
    return cached;
  }

  if (!force && cached && cached.lastTriedAt && Date.now() - cached.lastTriedAt < IMAGE_RETRY_MS) {
    return cached;
  }

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
      const empty = {
        mint,
        name: null,
        symbol: null,
        metadataUri: null,
        image: null,
        lastTriedAt: Date.now(),
      };
      state.metadataByMint.set(mint, empty);
      return empty;
    }

    const serializer = getMetadataAccountDataSerializer();
    const [metadata] = serializer.deserialize(metadataAccount.data);

    const metadataUri = trimNulls(metadata.uri);
    let image = null;

    if (metadataUri) {
      const jsonBody = await fetchMetadataJsonWithRetry(metadataUri, force ? 3 : 1);
      if (jsonBody) {
        image =
          jsonBody?.image ||
          jsonBody?.properties?.image ||
          jsonBody?.properties?.files?.find((f) =>
            String(f?.type || '').toLowerCase().startsWith('image/')
          )?.uri ||
          null;
      }

      if (!image) {
        // Fast fallback for common CDN layout: /metadata/{id}.json => /images/{id}.jpg
        image = metadataUri
          .replace('/metadata/', '/images/')
          .replace(/\.json$/i, '.jpg');
      }
    }

    const item = {
      mint,
      name: trimNulls(metadata.name),
      symbol: trimNulls(metadata.symbol),
      collection: metadata?.collection?.key ? String(metadata.collection.key) : null,
      metadataUri,
      image,
      lastTriedAt: Date.now(),
    };
    state.metadataByMint.set(mint, item);
    return item;
  } catch {
    const fallback = {
      mint,
      name: null,
      symbol: null,
      collection: null,
      metadataUri: null,
      image: null,
      lastTriedAt: Date.now(),
    };
    state.metadataByMint.set(mint, fallback);
    return fallback;
  }
}

function getKeyAt(ix, accounts, name) {
  const idx = ix.accountIndexByName[name];
  if (idx === undefined) {
    return null;
  }
  return accounts[idx] || null;
}

function mapIndexedAccountsToPubkeys(keyIndexes, keys) {
  if (!Array.isArray(keyIndexes)) {
    return [];
  }
  return keyIndexes.map((i) => keys[i]).filter(Boolean);
}

function normalizeKey(key) {
  if (typeof key === 'string') {
    return key;
  }
  if (key && typeof key.pubkey === 'string') {
    return key.pubkey;
  }
  if (key && typeof key.toBase58 === 'function') {
    return key.toBase58();
  }
  return String(key || '');
}

function normalizeInstructionData(data) {
  if (!data) {
    return Buffer.alloc(0);
  }
  if (Buffer.isBuffer(data)) {
    return data;
  }
  if (data?.type === 'Buffer' && Array.isArray(data.data)) {
    return Buffer.from(data.data);
  }
  if (typeof data === 'string') {
    return Buffer.from(bs58.decode(data));
  }
  return Buffer.alloc(0);
}

function applyListingEvent(event) {
  const prev = state.listingsByAddress.get(event.listingAddress);
  if (prev && prev.slot > event.slot) {
    return;
  }
  if (prev && prev.slot === event.slot && Number(prev.seq || 0) > Number(event.seq || 0)) {
    return;
  }
  state.listingsByAddress.set(event.listingAddress, {
    ...(prev || {}),
    ...event,
  });
}

async function processSignature(signature, seq = 0) {
  if (state.processedSignatures.has(signature)) {
    return;
  }

  let tx = null;
  for (let i = 0; i < 3; i += 1) {
    tx = await connection.getTransaction(signature, {
      commitment: 'confirmed',
      maxSupportedTransactionVersion: 0,
      encoding: 'json',
    });
    if (tx) {
      break;
    }
    await sleep(150 + i * 200);
  }

  state.processedSignatures.add(signature);
  if (!tx || !tx.transaction?.message) {
    return;
  }

  const staticKeys = (tx.transaction.message.staticAccountKeys || []).map(normalizeKey);
  const loadedWritable = (tx.meta?.loadedAddresses?.writable || []).map(normalizeKey);
  const loadedReadonly = (tx.meta?.loadedAddresses?.readonly || []).map(normalizeKey);
  const keys = [...staticKeys, ...loadedWritable, ...loadedReadonly];
  const instructions = tx.transaction.message.compiledInstructions || [];

  for (const instruction of instructions) {
    const programId = keys[instruction.programIdIndex];
    if (programId !== MARKETPLACE_PROGRAM_ID.toBase58()) {
      continue;
    }

    const accounts = mapIndexedAccountsToPubkeys(
      instruction.accountKeyIndexes,
      keys
    );
    if (!accounts.length) {
      continue;
    }

    const raw = normalizeInstructionData(instruction.data);
    if (!raw || raw.length < 8) {
      continue;
    }

    const disc = bs58.encode(raw.subarray(0, 8));
    const ix = state.instructionMap.get(disc);
    if (!ix) {
      continue;
    }

    const collection = getKeyAt(ix, accounts, 'collection');
    if (COLLECTION_FILTER && collection !== COLLECTION_FILTER) {
      continue;
    }

    const listingAddress = getKeyAt(ix, accounts, 'listing');
    const assetMint = getKeyAt(ix, accounts, 'asset');
    const seller = getKeyAt(ix, accounts, 'seller') || getKeyAt(ix, accounts, 'payer');
    const currencyMint = getKeyAt(ix, accounts, 'currency_mint');

    if (!listingAddress || !assetMint) {
      continue;
    }

    let status = 'Unknown';
    let priceUi = null;

    if (ix.name === 'list') {
      status = 'Active';
      if (raw.length >= 16) {
        const priceAtomic = Number(raw.readBigUInt64LE(8));
        priceUi = priceAtomic / 1e9;
      }
    }

    if (ix.name === 'unlist' || ix.name === 'buy') {
      status = ix.name === 'buy' ? 'Sold' : 'Unlisted';
    }

    const blockTime = tx.blockTime || 0;
    applyListingEvent({
      listingAddress,
      assetMint,
      seller,
      currencyMint,
      collection,
      priceUi,
      status,
      createdAt: blockTime,
      slot: tx.slot || 0,
      seq,
      signature,
    });
  }
}

async function processSignaturesBatch(signatures, source) {
  if (!signatures.length) {
    return 0;
  }

  // Apply in chronological order to preserve state transitions.
  const ordered = [...signatures].reverse();
  await mapConcurrent(
    ordered,
    state.txConcurrency,
    async (row, index) => processSignature(row.signature, index)
  );

  const newest = signatures[0]?.signature || null;
  const oldest = signatures[signatures.length - 1]?.signature || null;

  if (!state.newestSigSeen && newest) {
    state.newestSigSeen = newest;
  }
  if (!state.backfillBefore && oldest) {
    state.backfillBefore = oldest;
  }

  console.log(
    `[${source}] processed ${signatures.length} sigs | tracked listings: ${state.listingsByAddress.size} | txConcurrency=${state.txConcurrency}`
  );

  return signatures.length;
}

async function pollHead() {
  const sigs = await connection.getSignaturesForAddress(MARKETPLACE_PROGRAM_ID, {
    limit: HEAD_BATCH,
  });

  if (!sigs.length) {
    return;
  }

  if (!state.newestSigSeen) {
    await processSignaturesBatch(sigs, 'head:init');
    state.newestSigSeen = sigs[0].signature;
    state.backfillBefore = sigs[sigs.length - 1].signature;
    return sigs.length;
  }

  const fresh = [];
  for (const row of sigs) {
    if (row.signature === state.newestSigSeen) {
      break;
    }
    fresh.push(row);
  }

  if (!fresh.length) {
    return 0;
  }

  await processSignaturesBatch(fresh, 'head');
  state.newestSigSeen = sigs[0].signature;
  return fresh.length;
}

async function stepBackfill() {
  if (state.backfillDone || !state.backfillBefore) {
    return 0;
  }

  const sigs = await connection.getSignaturesForAddress(MARKETPLACE_PROGRAM_ID, {
    before: state.backfillBefore,
    limit: BACKFILL_BATCH,
  });

  if (!sigs.length) {
    state.backfillDone = true;
    console.log('[backfill] done');
    return 0;
  }

  await processSignaturesBatch(sigs, 'backfill');
  state.backfillBefore = sigs[sigs.length - 1].signature;
  return sigs.length;
}

async function runBackfillBurst() {
  const startedAt = Date.now();
  let steps = 0;
  let processed = 0;

  while (steps < state.backfillStepsPerTick && !state.backfillDone) {
    const count = await stepBackfill();
    if (!count) {
      break;
    }
    processed += count;
    steps += 1;
    if (Date.now() - startedAt >= BACKFILL_TIME_BUDGET_MS) {
      break;
    }
  }

  return { steps, processed };
}

function adaptOnSuccess() {
  state.successStreak += 1;
  if (state.successStreak < 3) {
    return;
  }

  state.successStreak = 0;
  if (state.txConcurrency < TX_CONCURRENCY) {
    state.txConcurrency += 1;
  }
  if (state.backfillStepsPerTick < BACKFILL_STEPS_PER_TICK) {
    state.backfillStepsPerTick += 1;
  }
}

function adaptOnError(errorMessage) {
  state.successStreak = 0;
  const msg = String(errorMessage || '').toLowerCase();
  const isRpcPressure =
    msg.includes('429') ||
    msg.includes('too many requests') ||
    msg.includes('rate limit') ||
    msg.includes('timeout') ||
    msg.includes('scan aborted');

  if (!isRpcPressure) {
    return;
  }

  state.txConcurrency = Math.max(2, state.txConcurrency - 2);
  state.backfillStepsPerTick = Math.max(1, state.backfillStepsPerTick - 1);
}

async function enrichMissingImagesBatch() {
  const active = [...state.listingsByAddress.values()].filter((x) => x.status === 'Active');
  const missingMints = active
    .filter((x) => {
      const md = state.metadataByMint.get(x.assetMint);
      if (!md) {
        return true;
      }
      if (md.image) {
        return false;
      }
      if (!md.lastTriedAt) {
        return true;
      }
      return Date.now() - md.lastTriedAt >= IMAGE_RETRY_MS;
    })
    .map((x) => x.assetMint);

  if (!missingMints.length) {
    return;
  }

  const unique = [...new Set(missingMints)].slice(0, ENRICH_PER_TICK);
  await mapConcurrent(unique, 8, async (mint) => fetchMetadataForMint(mint));
}

async function enrichPageListings(pageItems) {
  const mintsToForce = [...new Set(
    pageItems
      .filter((x) => {
        const md = state.metadataByMint.get(x.assetMint);
        return !md || !md.image;
      })
      .map((x) => x.assetMint)
  )];

  if (!mintsToForce.length) {
    return;
  }

  await mapConcurrent(mintsToForce, 12, async (mint) =>
    fetchMetadataForMint(mint, { force: true })
  );
}

function getMissingImageCount() {
  const active = [...state.listingsByAddress.values()].filter((x) => x.status === 'Active');
  return active.filter((x) => {
    const md = state.metadataByMint.get(x.assetMint);
    return !md || !md.image;
  }).length;
}

function getActiveListingsSorted() {
  const list = [...state.listingsByAddress.values()]
    .filter((x) => x.status === 'Active')
    .sort((a, b) => b.createdAt - a.createdAt);

  return list.map((item) => {
    const md = state.metadataByMint.get(item.assetMint) || null;
    return {
      ...item,
      name: md?.name || null,
      symbol: md?.symbol || null,
      metadataCollection: md?.collection || null,
      metadataUri: md?.metadataUri || null,
      image: md?.image || null,
    };
  });
}

function deriveCollectionLabelFromName(name) {
  const text = String(name || '').trim();
  if (!text) {
    return null;
  }
  const withoutNumber = text.replace(/\s*#\d+\s*$/i, '').trim();
  return withoutNumber || null;
}

function getCollectionLabelFromRow(row) {
  return (
    deriveCollectionLabelFromName(row?.name) ||
    row?.metadataCollection ||
    row?.collection ||
    'unknown'
  );
}

function parseListingStatusField(status) {
  if (!status || typeof status !== 'object') {
    return 'Unknown';
  }
  if (status.Active !== undefined) {
    return 'Active';
  }
  if (status.Unlisted !== undefined) {
    return 'Unlisted';
  }
  if (status.Sold !== undefined) {
    return 'Sold';
  }
  return 'Unknown';
}

async function decodeListingAccount(listingAddress) {
  if (!state.bootstrapped || !state.idl) {
    state.idl = await getOnchainIdl();
    state.instructionMap = toInstructionMap(state.idl);
    state.bootstrapped = true;
  }

  const listingPk = new PublicKey(listingAddress);
  const info = await connection.getAccountInfo(listingPk, 'confirmed');
  if (!info) {
    return null;
  }

  try {
    const coder = new BorshAccountsCoder(state.idl);
    const row = coder.decode('Listing', info.data);
    return {
      listingAddress: listingPk.toBase58(),
      seller: row?.seller?.toBase58?.() || null,
      assetMint: row?.asset?.toBase58?.() || null,
      collection: row?.collection?.toBase58?.() || null,
      currencyMint: row?.currency_mint?.toBase58?.() || null,
      status: parseListingStatusField(row?.status),
      priceUi: Number(asBigInt(row?.price || 0n)) / 1e9,
    };
  } catch {
    return null;
  }
}

async function detectActiveListingFromAccount(assetMint, expectedSeller) {
  if (!state.bootstrapped || !state.idl) {
    state.idl = await getOnchainIdl();
    state.instructionMap = toInstructionMap(state.idl);
    state.bootstrapped = true;
  }

  const coder = new BorshAccountsCoder(state.idl);
  const assetPk = new PublicKey(assetMint);
  const versionCounterPk = getListingVersionCounterPda(assetPk);
  const vcInfo = await connection.getAccountInfo(versionCounterPk, 'confirmed');
  if (!vcInfo) {
    return null;
  }

  let versionCounter;
  try {
    versionCounter = coder.decode('AssetListingVersionCounter', vcInfo.data);
  } catch {
    return null;
  }

  const current = asBigInt(versionCounter?.version_count || 0n);
  const candidates = [];
  for (let i = 0n; i < 3n; i += 1n) {
    const n = current - i;
    if (n < 0n) {
      break;
    }
    candidates.push(n);
  }

  for (const version of candidates) {
    const listingPk = getListingPda(assetPk, version);
    const listingInfo = await connection.getAccountInfo(listingPk, 'confirmed');
    if (!listingInfo) {
      continue;
    }

    try {
      const listing = coder.decode('Listing', listingInfo.data);
      const seller = listing?.seller?.toBase58?.() || null;
      const status = parseListingStatusField(listing?.status);
      if (status !== 'Active') {
        continue;
      }
      if (expectedSeller && seller !== expectedSeller) {
        continue;
      }

      return {
        listingAddress: listingPk.toBase58(),
        seller,
        assetMint,
        collection: listing?.collection?.toBase58?.() || null,
        currencyMint: listing?.currency_mint?.toBase58?.() || null,
        priceUi: Number(asBigInt(listing?.price || 0n)) / 1e9,
      };
    } catch {
      // ignore malformed candidate
    }
  }

  return null;
}

async function getWalletAssets(ownerAddress) {
  const owner = new PublicKey(ownerAddress);
  const tokenAccounts = await connection.getParsedTokenAccountsByOwner(
    owner,
    { programId: TOKEN_PROGRAM_ID },
    'confirmed'
  );

  const mintSet = new Set();
  for (const row of tokenAccounts.value || []) {
    const parsed = row?.account?.data?.parsed?.info;
    const amount = Number(parsed?.tokenAmount?.amount || '0');
    const decimals = Number(parsed?.tokenAmount?.decimals || 0);
    if (amount >= 1 && decimals === 0 && parsed?.mint) {
      mintSet.add(String(parsed.mint));
    }
  }

  const mints = [...mintSet];
  await mapConcurrent(mints, 12, async (mint) => fetchMetadataForMint(mint, { force: true }));

  const result = mints
    .map((mint) => {
      const md = state.metadataByMint.get(mint) || null;
      return {
        mint,
        name: md?.name || null,
        symbol: md?.symbol || null,
        image: md?.image || null,
        metadataUri: md?.metadataUri || null,
        metadataCollection: md?.collection || null,
      };
    })
    .sort((a, b) => String(a.name || a.mint).localeCompare(String(b.name || b.mint)));

  return result;
}

async function buildPortfolioForOwner(ownerAddress) {
  const owner = String(ownerAddress || '').trim();
  const owned = await getWalletAssets(owner);
  const activeByOwner = getActiveListingsSorted().filter((x) => x.seller === owner);

  // Fallback: for owned NFTs, detect active listing directly from listing PDA.
  const fallbackChecks = await mapConcurrent(owned, 8, async (asset) => {
    const found = await detectActiveListingFromAccount(asset.mint, owner);
    return found ? [asset.mint, found] : null;
  });
  const fallbackByMint = new Map(fallbackChecks.filter(Boolean));

  const ownedByMint = new Map(owned.map((x) => [x.mint, x]));
  const listedByMint = new Map(activeByOwner.map((x) => [x.assetMint, x]));

  const rows = [];

  for (const asset of owned) {
    const listed = listedByMint.get(asset.mint) || fallbackByMint.get(asset.mint) || null;
    rows.push({
      owner,
      mint: asset.mint,
      name: asset.name,
      symbol: asset.symbol,
      image: asset.image,
      collection: asset.metadataCollection,
      collectionLabel: getCollectionLabelFromRow({
        name: asset.name,
        metadataCollection: asset.metadataCollection,
      }),
      status: listed ? 'listed' : 'owned',
      listingAddress: listed?.listingAddress || null,
      priceUi: listed?.priceUi || null,
      seller: listed?.seller || owner,
      inEscrow: false,
    });
  }

  for (const listed of activeByOwner) {
    if (ownedByMint.has(listed.assetMint)) {
      continue;
    }
    rows.push({
      owner,
      mint: listed.assetMint,
      name: listed.name || null,
      symbol: listed.symbol || null,
      image: listed.image || null,
      collection: listed.collection || listed.metadataCollection || null,
      collectionLabel: getCollectionLabelFromRow({
        name: listed.name,
        metadataCollection: listed.metadataCollection,
        collection: listed.collection,
      }),
      status: 'listed',
      listingAddress: listed.listingAddress,
      priceUi: listed.priceUi,
      seller: listed.seller,
      inEscrow: true,
    });
  }

  rows.sort((a, b) => {
    if (a.status !== b.status) {
      return a.status === 'listed' ? -1 : 1;
    }
    return String(a.name || a.mint).localeCompare(String(b.name || b.mint));
  });

  return rows;
}

function buildCollectionStats(assets) {
  const active = getActiveListingsSorted();
  const neededCollections = new Set(
    assets
      .map((x) => x.collectionLabel || x.collection || null)
      .filter(Boolean)
  );

  const floorByCollection = new Map();
  const activeCountByCollection = new Map();
  for (const row of active) {
    const key = getCollectionLabelFromRow(row);
    if (!key || !neededCollections.has(key)) {
      continue;
    }
    const price = Number(row.priceUi || 0);
    if (!floorByCollection.has(key) || price < floorByCollection.get(key)) {
      floorByCollection.set(key, price);
    }
    activeCountByCollection.set(key, Number(activeCountByCollection.get(key) || 0) + 1);
  }

  const ownedCountByCollection = new Map();
  for (const item of assets) {
    const key = item.collectionLabel || item.collection || null;
    if (!key) {
      continue;
    }
    ownedCountByCollection.set(key, Number(ownedCountByCollection.get(key) || 0) + 1);
  }

  const result = {};
  for (const collection of neededCollections) {
    result[collection] = {
      collection,
      floorPriceUi: floorByCollection.has(collection) ? floorByCollection.get(collection) : null,
      activeListings: Number(activeCountByCollection.get(collection) || 0),
      myAssets: Number(ownedCountByCollection.get(collection) || 0),
    };
  }
  return result;
}

function buildNameStats(assets) {
  const active = getActiveListingsSorted();
  const neededNames = new Set(
    assets
      .map((x) => String(x.name || '').trim())
      .filter(Boolean)
  );

  const floorByName = new Map();
  const activeCountByName = new Map();
  for (const row of active) {
    const key = String(row.name || '').trim();
    if (!key || !neededNames.has(key)) {
      continue;
    }
    const price = Number(row.priceUi || 0);
    if (!floorByName.has(key) || price < floorByName.get(key)) {
      floorByName.set(key, price);
    }
    activeCountByName.set(key, Number(activeCountByName.get(key) || 0) + 1);
  }

  const myCountByName = new Map();
  for (const item of assets) {
    const key = String(item.name || '').trim();
    if (!key) {
      continue;
    }
    myCountByName.set(key, Number(myCountByName.get(key) || 0) + 1);
  }

  const stats = {};
  let totalEstimatedFloorValue = 0;
  for (const name of neededNames) {
    const floor = floorByName.has(name) ? floorByName.get(name) : null;
    const myAssets = Number(myCountByName.get(name) || 0);
    const estimatedFloorValue = floor == null ? 0 : floor * myAssets;
    totalEstimatedFloorValue += estimatedFloorValue;
    stats[name] = {
      name,
      floorPriceUi: floor,
      activeListings: Number(activeCountByName.get(name) || 0),
      myAssets,
      estimatedFloorValue,
    };
  }

  return {
    stats,
    totalEstimatedFloorValue,
  };
}

function applyFilterAndSort(listings, q, sort) {
  let result = listings;

  if (q) {
    result = result.filter((item) => {
      const name = String(item.name || '').toLowerCase();
      return name.includes(q);
    });
  }

  if (sort === 'price_asc') {
    result = result.sort((a, b) => Number(a.priceUi || 0) - Number(b.priceUi || 0));
  } else if (sort === 'price_desc') {
    result = result.sort((a, b) => Number(b.priceUi || 0) - Number(a.priceUi || 0));
  } else {
    result = result.sort((a, b) => Number(b.createdAt || 0) - Number(a.createdAt || 0));
  }

  return result;
}

async function refreshLoop() {
  if (state.loading) {
    return;
  }

  state.loading = true;
  try {
    if (!state.bootstrapped) {
      state.idl = await getOnchainIdl();
      state.instructionMap = toInstructionMap(state.idl);
      state.bootstrapped = true;
    }

    await pollHead();
    const burst = await runBackfillBurst();
    await enrichMissingImagesBatch();

    state.lastOkAt = new Date().toISOString();
    state.lastError = null;
    adaptOnSuccess();
    const activeCount = getActiveListingsSorted().length;
    const missingImageCount = getMissingImageCount();
    console.log(
      `[tick] active=${activeCount} missingImage=${missingImageCount} processed=${state.processedSignatures.size} backfillSteps=${burst.steps} txConcurrency=${state.txConcurrency}`
    );
  } catch (error) {
    state.lastError = String(error?.message || error);
    adaptOnError(state.lastError);
    console.error('[refresh] failed:', state.lastError);
  } finally {
    state.loading = false;
  }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);

  if (url.pathname === '/api/listings') {
    const page = Math.max(1, Number(url.searchParams.get('page') || '1'));
    const limit = Math.max(1, Math.min(200, Number(url.searchParams.get('limit') || DEFAULT_PAGE_SIZE)));
    const q = String(url.searchParams.get('q') || '').trim().toLowerCase();
    const sort = String(url.searchParams.get('sort') || 'newest');

    const active = applyFilterAndSort(getActiveListingsSorted(), q, sort);

    const start = (page - 1) * limit;
    const end = start + limit;
    const pageItems = active.slice(start, end);

    // Ensure items visible on the page are enriched aggressively.
    await enrichPageListings(pageItems);
    const freshActive = applyFilterAndSort(getActiveListingsSorted(), q, sort);
    const freshPageItems = freshActive.slice(start, end);

    return json(res, 200, {
      ok: state.bootstrapped,
      loading: state.loading,
      lastOkAt: state.lastOkAt,
      lastError: state.lastError,
      refreshMs: REFRESH_MS,
      page,
      limit,
      q,
      sort,
      total: freshActive.length,
      hasMore: end < freshActive.length,
      progress: {
        backfillDone: state.backfillDone,
        newestSigSeen: state.newestSigSeen,
        backfillBefore: state.backfillBefore,
        processedSignatures: state.processedSignatures.size,
        txConcurrency: state.txConcurrency,
        backfillStepsPerTick: state.backfillStepsPerTick,
      },
      data: {
        rpc: RPC_URL,
        programId: MARKETPLACE_PROGRAM_ID.toBase58(),
        collectionFilter: COLLECTION_FILTER,
        generatedAt: new Date().toISOString(),
        totalActiveListings: freshActive.length,
        listings: freshPageItems,
      },
    });
  }

  if (url.pathname === '/api/wallet/assets' && req.method === 'GET') {
    try {
      const owner = String(url.searchParams.get('owner') || '').trim();
      if (!owner) {
        return badRequest(res, 'owner is required');
      }

      const assets = await getWalletAssets(owner);
      return json(res, 200, {
        ok: true,
        owner,
        total: assets.length,
        data: {
          assets,
        },
      });
    } catch (error) {
      return json(res, 500, {
        ok: false,
        error: String(error?.message || error),
      });
    }
  }

  if (url.pathname === '/api/portfolio' && req.method === 'GET') {
    try {
      const ownersRaw = String(url.searchParams.get('owners') || '').trim();
      if (!ownersRaw) {
        return badRequest(res, 'owners is required');
      }

      const owners = [...new Set(
        ownersRaw
          .split(',')
          .map((x) => x.trim())
          .filter(Boolean)
      )].slice(0, 50);

      const byOwner = {};
      let combined = [];
      for (const owner of owners) {
        const rows = await buildPortfolioForOwner(owner);
        byOwner[owner] = rows;
        combined = combined.concat(rows);
      }

      const collectionStats = buildCollectionStats(combined);
      const nameStatsResult = buildNameStats(combined);

      return json(res, 200, {
        ok: true,
        owners,
        total: combined.length,
        data: {
          byOwner,
          assets: combined,
          collectionStats,
          nameStats: nameStatsResult.stats,
          summary: {
            totalEstimatedFloorValue: nameStatsResult.totalEstimatedFloorValue,
          },
        },
      });
    } catch (error) {
      return json(res, 500, {
        ok: false,
        error: String(error?.message || error),
      });
    }
  }

  if (url.pathname === '/api/refresh' && req.method === 'POST') {
    refreshLoop();
    return json(res, 202, { ok: true, queued: true });
  }

  if (url.pathname === '/api/tx/build' && req.method === 'POST') {
    try {
      const body = await readJsonBody(req);
      const action = String(body.action || '').trim();
      const wallet = String(body.wallet || '').trim();
      const payload = body.payload || {};

      if (!wallet) {
        return badRequest(res, 'wallet is required');
      }
      if (!action) {
        return badRequest(res, 'action is required');
      }

      const built = await buildUnsignedTxBase64({ wallet, action, payload });
      return json(res, 200, {
        ok: true,
        action,
        ...built,
      });
    } catch (error) {
      return json(res, 500, {
        ok: false,
        error: String(error?.message || error),
      });
    }
  }

  if (url.pathname === '/' || url.pathname === '/index.html') {
    return serveFile(res, path.join(PUBLIC_DIR, 'index.html'));
  }

  if (url.pathname === '/app.js') {
    return serveFile(res, path.join(PUBLIC_DIR, 'app.js'));
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

server.listen(PORT, HOST, () => {
  console.log(`Web live market: http://${HOST}:${PORT}`);
  console.log(`RPC: ${RPC_URL}`);
  console.log(`Program: ${MARKETPLACE_PROGRAM_ID.toBase58()}`);
  console.log(`Collection filter: ${COLLECTION_FILTER}`);
  console.log(
    `Indexer tuning: txConcurrency=${state.txConcurrency} backfillStepsPerTick=${state.backfillStepsPerTick} backfillBatch=${BACKFILL_BATCH}`
  );

  refreshLoop();
  setInterval(refreshLoop, REFRESH_MS);
});
