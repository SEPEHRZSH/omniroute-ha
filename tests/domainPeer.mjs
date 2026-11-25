import { MongoAdapter } from '../adapter/mongoAdapter.mjs';

const db = new MongoAdapter({ uri: process.env.HA_TEST_URI, database: process.env.HA_TEST_DATABASE });
globalThis.__omnirouteMongoAdapter = db;
const keys = await import('../upstream/omniroute/src/lib/db/apiKeys.ts');
const settings = await import('../upstream/omniroute/src/lib/db/settings.ts');
process.on('message', async message => {
  try {
    let value;
    if (message.operation === 'validate') value = await keys.validateApiKey(message.key);
    else if (message.operation === 'settings') value = await settings.getSettings();
    else if (message.operation === 'artifact') value = (await import('../upstream/omniroute/src/lib/usage/callLogArtifacts.ts')).readCallArtifact(message.path);
    else throw new Error('Unknown peer operation');
    process.send({ id: message.id, value });
  } catch (error) { process.send({ id: message.id, error: error.message }); }
});
process.on('disconnect', async () => { await db.close(); process.exit(0); });
process.send({ ready: true });
