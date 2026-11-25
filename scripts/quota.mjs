// Offline, evidence-backed settlement only. No blind refund/hold deletion and no
// traffic replay. The operator must verify the provider receipt out of band.
import { MongoClient } from 'mongodb';
import { randomUUID, createHash } from 'node:crypto';
import { readFileSync, statSync } from 'node:fs';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { workspacePath } from '../adapter/workspacePath.mjs';
const [command, database, argument, offline, ...extra] = process.argv.slice(2);
const poolModels=command==='pool-inspect'&&extra.length===1&&extra[0].startsWith('--models=')?extra[0].slice('--models='.length).split(','):undefined;
if (!process.env.HA_MONGO_URI || !/^ha_test_[a-z0-9_]+$/.test(database ?? '') || extra.length&&!poolModels ||
    !(command === 'status' && argument && !offline || ['reconcile','baseline-inspect','baseline-apply','team-inspect','team-apply','pool-inspect','pool-apply'].includes(command) && argument && offline === '--offline')) {
  throw new Error('Set HA_MONGO_URI; usage: quota.mjs status DATABASE RESERVATION_ID | reconcile DATABASE EVIDENCE_JSON --offline | baseline-inspect DATABASE KEY_ID --offline | baseline-apply DATABASE EVIDENCE_JSON --offline | team-inspect DATABASE TEAM_ID --offline | team-apply DATABASE EVIDENCE_JSON --offline | pool-inspect DATABASE POOL_ID --offline [--models=model-a,model-b] | pool-apply DATABASE EVIDENCE_JSON --offline');
}
if (command === 'status') {
  if (!/^[a-f0-9-]{36}$/.test(argument)) throw new Error('Invalid reservation identity');
  const client = new MongoClient(process.env.HA_MONGO_URI, { maxPoolSize: 1, minPoolSize: 0, serverSelectionTimeoutMS: 5000, timeoutMS: 5000, readConcern: { level: 'majority' }, readPreference: 'primary' });
  try {
    await client.connect();
    const row = await client.db(database).collection('_ha_reservations').findOne({ _id: argument }, { projection: { keyId: 1, provider: 1, model: 1, state: 1, bound: 1, epoch: 1, createdAt: 1, dispatchedAt: 1, settledAt: 1 } });
    if (!row) throw new Error('Unknown reservation');
    console.log(JSON.stringify(row));
  } finally { await client.close(); }
} else {
  if(command.startsWith('baseline-')||command.startsWith('team-')||command.startsWith('pool-')){
    const db=new MongoAdapter({uri:process.env.HA_MONGO_URI,database,ownerId:'offline-reconciliation-'+randomUUID(),allowReconciliation:true,timeoutMs:60000});let claimed=false;
    try{
      if(!db.ownership('claim',60000))throw new Error('Stop both applications and wait for their leases/transactions before baseline review');claimed=true;
      if(command.endsWith('-inspect'))console.log(JSON.stringify(db.reservation(command==='team-inspect'?'teamBaselineInspect':command==='pool-inspect'?'poolBaselineInspect':'baselineInspect',{id:randomUUID(),...(command==='team-inspect'?{teamId:argument}:command==='pool-inspect'?{poolId:argument,models:poolModels}:{keyId:argument})})));
      else{
        const file=workspacePath(argument);if(statSync(file).size>1048576)throw new Error('Baseline evidence exceeds 1 MiB');
        const bytes=readFileSync(file),evidence=JSON.parse(bytes);
        console.log(JSON.stringify(db.reservation(command==='team-apply'?'teamBaselineApply':command==='pool-apply'?'poolBaselineApply':'baselineApply',{id:evidence.id,evidence,evidenceSha256:createHash('sha256').update(bytes).digest('hex')})));
      }
    }finally{if(claimed&&db.open)db.ownership('release');await db.close();}
  }else{
  const file = workspacePath(argument);
  if (statSync(file).size > 16384) throw new Error('Evidence must be a small workspace file outside upstream');
  const bytes = readFileSync(file), evidence = JSON.parse(bytes);
  if (!/^[a-f0-9-]{36}$/.test(evidence.reservationId ?? '') ||
      Object.keys(evidence).some(key => !['reservationId','provider','model','providerReceiptId','verifiedBy','receipt'].includes(key))) throw new Error('Invalid provider evidence fields');
  const db = new MongoAdapter({ uri: process.env.HA_MONGO_URI, database, ownerId: 'offline-reconciliation-' + randomUUID(), allowReconciliation: true });
  let claimed = false;
  try {
    if (!db.ownership('claim',60000)) throw new Error('Stop both applications and wait for their leases/transactions before reconciliation');
    claimed = true;
    const result = db.reservation('reconcile', { id: evidence.reservationId, receipt: evidence.receipt,
      evidence: { provider: evidence.provider, model: evidence.model, providerReceiptId: evidence.providerReceiptId, verifiedBy: evidence.verifiedBy,
        sha256: createHash('sha256').update(bytes).digest('hex') } });
    if (!result.settled) throw new Error('Liability remains unresolved');
    console.log(JSON.stringify({ settled: true, reservationId: evidence.reservationId, evidenceSha256: createHash('sha256').update(bytes).digest('hex') }));
  } finally { if (claimed && db.open) db.ownership('release'); await db.close(); }
  }
}
