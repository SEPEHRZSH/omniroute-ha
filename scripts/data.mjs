// Explicit offline migration/backup command. No application runner, Docker, or
// production access; source/target names remain restricted to isolated staging.
import { MongoClient } from 'mongodb';
import { backupDatabase, restoreDatabase, importSqlite, inspectArchive,repairProviderRetirement } from '../adapter/dataTransfer.mjs';
const rebuildMemory = process.argv.includes('--rebuild-memory');
const repairRetirement=process.argv.includes('--repair-retirement');
const sourceArgs=process.argv.slice(2).filter(arg=>arg.startsWith('--quiescent-source='));
if(sourceArgs.length>1)throw new Error('Specify only one quiescent source');
const quiescentSource=sourceArgs[0]?.slice('--quiescent-source='.length);
const [command, ...args] = process.argv.slice(2).filter(arg => !['--rebuild-memory','--repair-retirement'].includes(arg)&&!sourceArgs.includes(arg));
if (rebuildMemory && command !== 'import-sqlite') throw new Error('--rebuild-memory is only valid with import-sqlite');
if(repairRetirement&&command!=='restore')throw new Error('--repair-retirement is only valid with restore');
if(sourceArgs.length&&(command!=='restore'||!/^ha_test_[a-z0-9_]+$/.test(quiescentSource)))throw new Error('--quiescent-source requires restore and an isolated source database');
if (command === 'inspect' && args.length === 1) {
  const { header, footer } = inspectArchive(args[0]);
  console.log(JSON.stringify({ database: header.database, createdAt: header.createdAt, collections: header.collections.map(c => c.name), counts: footer.counts, verified: true }));
} else {
  if (!process.env.HA_MONGO_URI || args.length !== (command==='repair-provider-retirement'?1:2) || !['backup', 'restore', 'import-sqlite','repair-provider-retirement'].includes(command)) {
    throw new Error('Set HA_MONGO_URI; usage: data.mjs backup DATABASE FILE | restore FILE NEW_DATABASE [--repair-retirement] [--quiescent-source=SOURCE_DATABASE] | import-sqlite FILE NEW_DATABASE [--rebuild-memory] | repair-provider-retirement DATABASE | inspect FILE');
  }
  const client = new MongoClient(process.env.HA_MONGO_URI, { maxPoolSize: 2, minPoolSize: 0, maxConnecting: 1,
    serverSelectionTimeoutMS: 5000, socketTimeoutMS: 10000, readPreference: 'primary', writeConcern: { w: 'majority', j: true, wtimeoutMS: 5000 } });
  try {
    await client.connect();
    if (!(await client.db('admin').command({ hello: 1 })).setName) throw new Error('Replica set required');
    const result = command==='repair-provider-retirement'?await repairProviderRetirement(client,process.env.HA_MONGO_URI,args[0]):command === 'backup' ? await backupDatabase(client, args[0], args[1])
      : command === 'restore' ? await restoreDatabase(client, args[0], args[1],{repairRetirement,uri:process.env.HA_MONGO_URI,quiescentSource})
        : await importSqlite(client, process.env.HA_MONGO_URI, args[0], args[1], { rebuildMemory });
    console.log(JSON.stringify(result));
  } finally { await client.close(); }
}
