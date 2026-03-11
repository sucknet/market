const { Connection, PublicKey } = require('@solana/web3.js');

const RPC = 'https://mainnet.fogo.io';
const PROGRAM_ID = new PublicKey('orcJCdDGwunzjWNJqwYJU2VLceao85qwkeKNf9Yos6T');
const SIG = process.argv[2] || '3wdg7gW7PKQfp8M4YbSV95rko7Zx6sFnqvyzHNLCWeZYbeT3ncpkXzVeuPDPJJCGGVaToZFMFHUj9qq4pbC6PMxi';

async function main() {
  const c = new Connection(RPC, 'confirmed');
  const tx = await c.getTransaction(SIG, { maxSupportedTransactionVersion: 0, commitment: 'confirmed' });
  if (!tx) {
    console.log('Transaction not found');
    return;
  }

  const staticKeys = tx.transaction.message.staticAccountKeys || [];
  const loadedWritable = tx.meta?.loadedAddresses?.writable || [];
  const loadedReadonly = tx.meta?.loadedAddresses?.readonly || [];
  const allKeys = [...staticKeys, ...loadedWritable, ...loadedReadonly];

  console.log('signature:', SIG);
  console.log('static keys:', staticKeys.length);
  console.log('loaded writable:', loadedWritable.length);
  console.log('loaded readonly:', loadedReadonly.length);
  console.log('total key space:', allKeys.length);

  const ixs = tx.transaction.message.compiledInstructions || [];
  let found = false;
  for (let i = 0; i < ixs.length; i += 1) {
    const ix = ixs[i];
    const pid = allKeys[ix.programIdIndex];
    if (!pid || pid.toBase58() !== PROGRAM_ID.toBase58()) continue;

    found = true;
    const discHex = Buffer.from(ix.data, 'base64').subarray(0, 8).toString('hex');
    console.log('\nMarketplace instruction index:', i);
    console.log('discriminator hex:', discHex);
    console.log('accounts len:', ix.accountKeyIndexes.length);

    ix.accountKeyIndexes.forEach((k, idx) => {
      const key = allKeys[k];
      console.log(`${idx}: keyIndex=${k} ${key ? key.toBase58() : 'unknown'}`);
    });
  }

  if (!found) {
    console.log('No marketplace instruction found in tx');
  }

  const logs = tx.meta?.logMessages || [];
  console.log('\n--- logs ---');
  for (const l of logs) {
    console.log(l);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
