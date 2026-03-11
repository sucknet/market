const { Connection, PublicKey } = require('@solana/web3.js');

async function main() {
  const connection = new Connection('https://mainnet.fogo.io', 'confirmed');
  const programId = new PublicKey('orcJCdDGwunzjWNJqwYJU2VLceao85qwkeKNf9Yos6T');
  const listDiscHex = 'eec8f4b8cda9f964';

  const sigs = await connection.getSignaturesForAddress(programId, { limit: 1000 });
  for (const s of sigs) {
    const tx = await connection.getTransaction(s.signature, {
      maxSupportedTransactionVersion: 0,
      commitment: 'confirmed',
    });
    if (!tx || tx.meta?.err) continue;

    const staticKeys = tx.transaction.message.staticAccountKeys || tx.transaction.message.accountKeys || [];
    const loadedWritable = tx.meta?.loadedAddresses?.writable || [];
    const loadedReadonly = tx.meta?.loadedAddresses?.readonly || [];
    const accountKeys = [...staticKeys, ...loadedWritable, ...loadedReadonly];

    const compiledInstructions = tx.transaction.message.compiledInstructions || [];
    for (const ix of compiledInstructions) {
      const ixProgram = accountKeys[ix.programIdIndex];
      if (!ixProgram || ixProgram.toBase58() !== programId.toBase58()) continue;

      const data = Buffer.from(ix.data, 'base64');
      if (data.length < 8) continue;
      if (data.subarray(0, 8).toString('hex') !== listDiscHex) continue;

      console.log(`signature: ${s.signature}`);
      console.log(`accountCount: ${ix.accountKeyIndexes.length}`);
      ix.accountKeyIndexes.forEach((idx, i) => {
        console.log(`${i}: ${accountKeys[idx].toBase58()}`);
      });
      return;
    }
  }

  console.log('No successful list instruction found.');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
