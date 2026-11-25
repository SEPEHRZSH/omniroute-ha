// Conservative, single-attempt chat liabilities. MongoDB transactions/fencing
// surround every operation; dispatched liabilities never expire or auto-refund.
import { createHash } from 'node:crypto';
import { checkQuotaPolicy } from './requestLimits.mjs';
import { poolLiabilities, settlePools } from './poolQuota.mjs';
import { getBudgetWindow } from './budgetCalendar.mjs';
import { checkScopedTokens, settleScopedTokens,scopedTokenSpecification } from './scopedTokens.mjs';
import {quotaBaselineOperation} from './quotaBaseline.mjs';
import { checkKeyUsage } from './keyUsage.mjs';
import { teamLiability } from './teamQuota.mjs';
const fail = (code, message) => { throw Object.assign(new Error(message), { code }); };
const receiptSignature = value => createHash('sha256').update(JSON.stringify({ input: value?.input, output: value?.output,
  reasoning: value?.reasoning ?? 0, tokens: value?.tokens, nanoUsd: value?.nanoUsd, priced: value?.priced,
  requestId: value?.requestId ?? null, serviceTier: value?.serviceTier ?? null })).digest('hex');
export function quotaBounds(value) {
  if (!value) return {};
  const bounds = typeof value === 'string' ? JSON.parse(value) : value;
  if (!bounds || Array.isArray(bounds) || Object.keys(bounds).length > 64) throw new TypeError('Invalid HA_QUOTA_BOUNDS');
  for (const [target, bound] of Object.entries(bounds)) {
    if (!/^openai-compatible-[A-Za-z0-9_-]+\/.{1,200}$/.test(target) ||
        !Number.isSafeInteger(bound.tokens) || bound.tokens < 1 || bound.tokens > 10000000 ||
        !Number.isSafeInteger(bound.nanoUsd) || bound.nanoUsd < 1 || bound.nanoUsd > 1000000000000 ||
        Object.keys(bound).some(k => !['tokens','nanoUsd','tools','maxImages','maxAudioBytes','mediaPricing','defaultOutputTokens'].includes(k)) ||
        bound.defaultOutputTokens!=null&&(!Number.isSafeInteger(bound.defaultOutputTokens)||bound.defaultOutputTokens<1||bound.defaultOutputTokens>bound.tokens) ||
        bound.tools!=null&&typeof bound.tools!=='boolean' ||
        ['maxImages','maxAudioBytes'].some(k=>bound[k]!=null&&(!Number.isSafeInteger(bound[k])||bound[k]<1||bound[k]>(k==='maxImages'?64:16777216))) ||
        bound.mediaPricing!=null&&!['tokens','provider'].includes(bound.mediaPricing) ||
        (bound.maxImages||bound.maxAudioBytes)&&!bound.mediaPricing) throw new TypeError('Invalid certified quota liability bound');
  }
  return Object.freeze(Object.fromEntries(Object.entries(bounds).map(([target, bound]) => [target, Object.freeze({ ...bound })])));
}
export const nanoUsd = value => {
  if (!Number.isFinite(value) || value < 0 || !Number.isSafeInteger(Math.ceil(value * 1e9))) throw new TypeError('Invalid USD amount');
  return Math.ceil(value * 1e9);
};
export async function reservationOperation(db, session, request, execute, bounds, schema, allowReconciliation = false) {
  const reservations = db.collection('_ha_reservations'), options = { session };
  const [{ now }] = await db.aggregate([{ $documents: [{}] }, { $project: { now: { $toLong: '$$NOW' } } }], options).toArray();
  const writer = await db.collection('_ha_serial').findOne({ _id: 'writer' }, options);
  if (!writer.ownerId) fail('HA_FENCED', 'Reservation requires active ownership');
  const id = request.id;
  if (typeof id !== 'string' || !/^[a-f0-9-]{36}$/.test(id)) throw new TypeError('Invalid reservation identity');
  if(['baselineInspect','baselineApply','teamBaselineInspect','teamBaselineApply','poolBaselineInspect','poolBaselineApply'].includes(request.action))return quotaBaselineOperation(db,session,request,{writer,now,execute,schema,allowAdmin:allowReconciliation,specification:scopedTokenSpecification,nanoUsd});
  const row = await reservations.findOne({ _id: id }, options);
  const reconciling = request.action === 'reconcile';
  if (reconciling && (!allowReconciliation || !/^[a-f0-9]{64}$/.test(request.evidence?.sha256 ?? '') ||
      !['verifiedBy','providerReceiptId'].every(field => typeof request.evidence?.[field] === 'string' && request.evidence[field].trim().length > 0 && request.evidence[field].length <= 200))) {
    fail('HA_QUOTA_POLICY', 'Reconciliation requires the offline administrative connection and verified provider evidence');
  }
  if (reconciling && (!row || request.evidence.provider !== row.provider || request.evidence.model !== row.model)) fail('HA_QUOTA_POLICY', 'Provider evidence does not match the reserved provider/model');
  if (['reserve', 'dispatch'].includes(request.action)) {
    const key = await db.collection('api_keys').findOne({ id: request.keyId ?? row?.keyId }, options);
    if (!key || key.is_active === 0 || key.is_banned === 1 || key.revoked_at || key.expires_at && Date.parse(key.expires_at) <= now) fail('HA_QUOTA_KEY_INVALID', 'API key is unavailable for provider dispatch');
  }
  if (request.action === 'reserve') {
    await schema('request_cost_ledger'); await schema('api_key_quota_counters');
    const { keyId, provider, model, connectionId } = request;
    const bound = bounds[provider + '/' + model];
    if (!bound) fail('HA_RESERVATION_REQUIRED', 'No certified liability bound for this provider/model');
    if (row) fail('HA_RESERVATION_CONFLICT', 'Reservation identity already exists');
    const quota = await db.collection('api_key_quota_limits').findOne({ api_key_id: keyId }, options);
    if (!(await checkQuotaPolicy(db, session, keyId, true)).managed) fail('HA_RESERVATION_REQUIRED', 'Quota policy changed before reservation');
    if (await db.collection('_ha_quota_blocked').findOne({ _id: keyId }, options)) fail('HA_QUOTA_UNCERTAIN', 'Quota requires operator reconciliation');
    // A new epoch can refund an intent that never dispatched. The old epoch
    // cannot subsequently mark it dispatched because the same TX fence applies.
    await reservations.updateMany({ state: 'reserved', epoch: { $lt: writer.epoch } }, { $set: { state: 'cancelled', recoveredAt: now } }, options);
    const held = await reservations.aggregate([{ $match: { keyId, state: { $in: ['reserved','dispatched','uncertain'] } } },
      { $group: { _id: null, tokens: { $sum: '$bound.tokens' }, money: { $sum: '$bound.nanoUsd' } } }], options).next();
    const checkSpend = async (start, limit, namespace) => {
      if (!Number.isFinite(limit) || !Number.isSafeInteger(Math.floor(limit * 1e9))) fail('HA_QUOTA_POLICY', 'Invalid hard USD limit');
      const unpriced = await db.collection('key_value').findOne({ namespace, key: keyId }, options);
      if (unpriced && (!Number.isFinite(Number(unpriced.value)) || Number(unpriced.value) >= start)) fail('HA_QUOTA_UNCERTAIN', 'Unpriced usage closes this quota period');
      const spent = await db.collection('domain_cost_history').aggregate([{ $match: { api_key_id: keyId, timestamp: { $gte: start } } },
        { $group: { _id: null, n: { $sum: { $ceil: { $multiply: ['$cost', 1000000000] } } }, min: { $min: '$cost' } } }], options).next();
      if (spent && (!Number.isSafeInteger(spent.n) || spent.min < 0)) fail('HA_QUOTA_UNCERTAIN', 'Invalid persisted USD accounting');
      if ((spent?.n ?? 0) + (held?.money ?? 0) + bound.nanoUsd > Math.floor(limit * 1e9)) fail('HA_QUOTA_EXCEEDED', 'Hard USD quota has insufficient unreserved capacity');
    };
    if (quota?.tpm_limit > 0) {
      // Full adjacent buckets deliberately overestimate a sliding minute. A
      // dispatched liability also remains held across every bucket boundary.
      const bucket = Math.floor(now / 60000);
      const used = await db.collection('api_key_quota_counters').aggregate([{ $match: { api_key_id: keyId, dimension_key: 'key-quota:tpm', bucket_index: { $gte: bucket - 1 } } }, { $group: { _id: null, n: { $sum: '$consumed' }, min: { $min: '$consumed' } } }], options).next();
      if (used && (!Number.isSafeInteger(used.n) || used.min < 0)) fail('HA_QUOTA_UNCERTAIN', 'Invalid persisted token accounting');
      if ((used?.n ?? 0) + (held?.tokens ?? 0) + bound.tokens > quota.tpm_limit) fail('HA_QUOTA_EXCEEDED', 'Hard token quota has insufficient unreserved capacity');
    }
    if (quota?.monthly_amount_usd > 0) {
      const date = new Date(now), start = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), 1);
      // Spend from all endpoints is authoritative here. Managed settlement
      // publishes its charge and releases its hold in this same transaction.
      await checkSpend(start, quota.monthly_amount_usd, 'keyQuotaUnpricedUsage');
    }
    const budget = await db.collection('domain_budgets').findOne({ api_key_id: keyId }, options);
    if (budget) {
      const rawInterval = typeof budget.reset_interval === 'string' ? budget.reset_interval.trim().toLowerCase() : '';
      const interval = ['daily','weekly','monthly'].includes(rawInterval) ? rawInterval : 'daily';
      // Upstream selects one active interval, with daily as its legacy fallback.
      const limit = budget[interval + '_limit_usd'] > 0 ? budget[interval + '_limit_usd'] : budget.daily_limit_usd;
      if (limit > 0) await checkSpend(getBudgetWindow(interval, budget.reset_time ?? '00:00', now).periodStartAt, limit, 'budgetUnpricedUsage');
    }
    const usageWindows=await checkKeyUsage(db,session,keyId,now,execute,checkSpend);
    const teamId=await teamLiability(db,session,keyId,bound,now);
    const pools = await poolLiabilities(db, session, { keyId, connectionId, provider, model, bound, now });
    await checkScopedTokens(db,session,{keyId,provider,model,bound},now,schema);
    await reservations.insertOne({ _id: id, keyId, provider, model, connectionId: connectionId ?? null, bound, ...pools, ...(usageWindows?{usageWindows}:{}), ...(teamId?{teamId}:{}), epoch: writer.epoch, state: 'reserved', createdAt: now }, options);
    return { id, bound };
  }
  if (!row) fail('HA_RESERVATION_CONFLICT', 'Unknown reservation');
  if (request.action === 'cancel') {
    if (row.state === 'reserved') await reservations.updateOne({ _id: id }, { $set: { state: 'cancelled', cancelledAt: now } }, options);
    return;
  }
  if (request.action === 'dispatch') {
    if (row.epoch !== writer.epoch || row.state !== 'reserved') fail('HA_RESERVATION_CONFLICT', 'Additional or stale provider dispatch refused');
    await reservations.updateOne({ _id: id }, { $set: { state: 'dispatched', dispatchedAt: now } }, options); return;
  }
  if (request.action !== 'settle' && !reconciling) throw new TypeError('Invalid reservation action');
  const { receipt } = request;
  const reasoning = receipt?.reasoning ?? 0;
  const valid = Number.isSafeInteger(receipt?.tokens) && receipt.tokens >= 0 && Number.isSafeInteger(receipt?.input) && receipt.input >= 0 &&
    Number.isSafeInteger(receipt?.output) && receipt.output >= 0 && Number.isSafeInteger(reasoning) && reasoning >= 0 && receipt.tokens === receipt.input + receipt.output + reasoning &&
    Number.isSafeInteger(receipt?.nanoUsd) && receipt.nanoUsd >= 0 && receipt.priced === true &&
    ['requestId','serviceTier'].every(field => receipt[field] == null || typeof receipt[field] === 'string' && receipt[field].length <= 2000);
  const signature = receiptSignature(receipt);
  const recordEvidence = async () => {
    if (!reconciling) return;
    await db.collection('_ha_quota_reconciliations').updateOne({ _id: id }, { $setOnInsert: { ...request.evidence, receiptSignature: signature,
      previousState: row.state, keyId: row.keyId, epoch: writer.epoch, verifiedAt: now } }, { ...options, upsert: true });
    await reservations.updateOne({_id:id},{$unset:{accountingConflict:''}},options);
    const blocked = await db.collection('_ha_quota_blocked').findOne({ _id: row.keyId }, options);
    // Old or unrelated quarantine records are never guessed away. Each known
    // conflicting reservation must be individually verified before reopening.
    if (Array.isArray(blocked?.reservationIds) && blocked.reservationIds.includes(id)) {
      await db.collection('_ha_quota_blocked').updateOne({ _id: row.keyId }, { $pull: { reservationIds: id } }, options);
      await db.collection('_ha_quota_blocked').deleteOne({ _id: row.keyId, reservationIds: { $size: 0 }, reason: 'Uncertain or conflicting provider liability' }, options);
    }
  };
  if (valid && row.state === 'settled' && (signature === row.receiptSignature || row.receipt && signature === receiptSignature(row.receipt))) { await recordEvidence(); return { settled: true }; }
  if (!valid || !(row.state === 'dispatched' || reconciling && row.state === 'uncertain') || receipt.tokens > row.bound.tokens || receipt.nanoUsd > row.bound.nanoUsd) {
    if (reconciling) fail('HA_QUOTA_UNCERTAIN', 'Provider evidence cannot settle this liability; preserve the hold and quarantine');
    await db.collection('_ha_quota_blocked').updateOne({ _id: row.keyId }, { $set: { reason: 'Uncertain or conflicting provider liability', updatedAt: now }, $addToSet: { reservationIds: id } }, { ...options, upsert: true });
    await reservations.updateOne({_id:id},{$set:{accountingConflict:true}},options);
    if (row.state !== 'settled') await reservations.updateOne({ _id: id }, { $set: { state: 'uncertain', observedAt: now } }, options);
    return { settled: false }; // Commit quarantine; never release an uncertain hold.
  }
  const run = (sql, args) => execute({ sql, args, mode: 'run' });
  await settleScopedTokens(db,session,row,receipt,now,schema,execute);
  await run('INSERT INTO domain_cost_history(api_key_id,cost,timestamp) VALUES (?,?,?)', [row.keyId, receipt.nanoUsd / 1e9, now]);
  await run('INSERT INTO request_cost_ledger(api_key_id,provider,model,tokens_input,tokens_output,tokens_reasoning,amount_usd,timestamp,request_id,service_tier) VALUES (?,?,?,?,?,?,?,?,?,?)',
    [row.keyId, row.provider, row.model, receipt.input, receipt.output, reasoning, receipt.nanoUsd / 1e9, new Date(now).toISOString(), receipt.requestId ?? id, receipt.serviceTier ?? 'standard']);
  await run('INSERT INTO api_key_quota_counters(api_key_id,dimension_key,bucket_index,consumed,updated_at) VALUES (?,?,?,?,?) ON CONFLICT(api_key_id,dimension_key,bucket_index) DO UPDATE SET consumed=consumed+excluded.consumed,updated_at=excluded.updated_at',
    [row.keyId, 'key-quota:tpm', Math.floor(now / 60000), receipt.tokens, now]);
  await settlePools(db, session, row, receipt, now);
  await reservations.updateOne({ _id: id }, { $set: { state: 'settled', settledAt: now, receipt, receiptSignature: signature },...(reconciling?{$unset:{accountingConflict:''}}:{}) }, options);
  await recordEvidence();
  return { settled: true };
}
