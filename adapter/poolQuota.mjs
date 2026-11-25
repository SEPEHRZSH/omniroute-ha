// Native reservation checks reuse the official pure decision/calendar functions.
// All mutable policy, counters and outstanding liabilities are read in the same
// MongoDB session/fence; the upstream fail-open hot path is not the authority.
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { isDeepStrictEqual } from 'node:util';
import { parse, splitStatements } from './sql.mjs';
import { compileTable } from './schema.mjs';
async function pinned(file, hash) {
  const url = new URL('../upstream/omniroute/src/lib/quota/' + file + '.ts', import.meta.url);
  if (createHash('sha256').update(readFileSync(url)).digest('hex') !== hash) throw new Error('Quota library source changed: ' + file);
  return import(url);
}
const { decideFairShare } = await pinned('fairShare', '5dc6b8379682b185c10ac48f301f8ef36923a9aa0edab219ae06b92c879d4c3d');
const { getKnownPlan } = await pinned('planRegistry', 'b4a8046120d86af91b174607d8e88b9d7fb177b78224a3ff849b05cacb060d36');
const { resolveActiveSchedule, scheduleBucketPoolId } = await pinned('schedules', 'b8ca7727d5d2f8be5baecbbddb29258d130ea91e8010d5865112ca0725165156');
export const windows = Object.freeze({ hourly: 3600000, '5h': 18000000, daily: 86400000, weekly: 604800000, monthly: 2592000000 });
const units = ['requests', 'tokens', 'usd'];
const consumptionSql = readFileSync(new URL('../upstream/omniroute/src/lib/db/migrations/078_quota_consumption.sql', import.meta.url), 'utf8');
if (createHash('sha256').update(consumptionSql).digest('hex') !== 'a22760383f409b8c0733d4b0129149e4966e13da6029bc6a9cdaa0a9ce7f80c4') throw new Error('Pool consumption schema changed');
const expectedConsumption = compileTable(parse(splitStatements(consumptionSql)[0]).ast);
function normalized(value) {
  if (value == null) return null;
  if (Array.isArray(value)) return value.map(normalized);
  return typeof value === 'object' ? Object.fromEntries(Object.entries(value).map(([key, entry]) => [key, normalized(entry)])) : value;
}
async function consumptionSchema(db, session) {
  const spec = await db.collection('_ha_schema').findOne({ _id: 'quota_consumption' }, { session });
  if (!isDeepStrictEqual(normalized(spec), normalized(expectedConsumption))) fail('HA_QUOTA_POLICY', 'Pool consumption schema is incompatible');
}
export {consumptionSchema as assertPoolConsumptionSchema,scheduleBucketPoolId as poolScheduleNamespace};
const fail = (code, message) => { throw Object.assign(new Error(message), { code }); };
const json = text => { try { return JSON.parse(text); } catch { fail('HA_QUOTA_POLICY', 'Invalid pool policy JSON'); } };
const integer = n => { if (!Number.isSafeInteger(n) || n < 0) fail('HA_QUOTA_UNCERTAIN', 'Invalid persisted pool accounting'); return n; };
const amount = (bound, unit) => unit === 'requests' ? 1 : unit === 'tokens' ? bound.tokens : bound.nanoUsd;
function exactUsd(row) {
  if (row.consumed===0 && row._ha_usd_nano==null) return 0;
  if (!Number.isSafeInteger(row._ha_usd_nano) || row._ha_usd_nano<0) fail('HA_QUOTA_POLICY','USD pool usage requires verified integer accounting');
  return row._ha_usd_nano;
}
function usdFilters(keyId,namespaces,now) {
  return namespaces.flatMap(namespace=>Object.entries(windows).map(([window,ms])=>({api_key_id:keyId,dimension_key:`${namespace}:usd:${window}`,bucket_index:Math.floor(now/ms)})));
}
const limit = (n, unit) => {
  if (!Number.isFinite(n) || n < 0) fail('HA_QUOTA_POLICY', 'Invalid pool quota limit');
  return integer(unit === 'usd' ? Math.floor(n * 1e9) : Math.floor(n));
};

export async function poolLiabilities(db, session, { keyId, connectionId, provider, model, bound, now }) {
  const options = { session }, matched = [], schedules = [], dimensions = [];
  const allocations = await db.collection('quota_allocations').find({ api_key_id: keyId }, options).limit(65).toArray();
  if (allocations.length > 64) fail('HA_QUOTA_POLICY', 'Too many pool allocations');
  for (const allocation of allocations) {
    const pool = await db.collection('quota_pools').findOne({ id: allocation.pool_id }, options);
    if (!pool) fail('HA_QUOTA_POLICY', 'Quota allocation has no pool');
    const members = await db.collection('quota_pool_connections').find({ pool_id: pool.id }, options).limit(65).toArray();
    const ids = [...new Set([pool.connection_id, ...members.map(m => m.connection_id)])];
    if (ids.length > 64 || ids.some(id => typeof id !== 'string' || !id)) fail('HA_QUOTA_POLICY', 'Invalid pool members');
    if (!ids.includes(connectionId)) continue;
    if(await db.collection('_ha_reservations').findOne({poolIds:pool.id,$or:[{state:'uncertain'},{accountingConflict:true}]},options)) fail('HA_QUOTA_UNCERTAIN','Shared pool has unresolved provider accounting');
    await consumptionSchema(db, session);
    const profiles = db.collection('_ha_pool_profiles');
    const profile = await profiles.findOne({ _id: pool.id }, options);
    if (!profile) {
      // Historical SQLite counters may cover only the old plan's windows. Do
      // not invent a complete baseline for a later dimension/model-cap change.
      // A fresh pool starts complete tracking before its first admitted call.
      const prefix = pool.id.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + ':';
      if (await db.collection('quota_consumption').findOne({ dimension_key: { $regex: '^' + prefix }, consumed: { $gt: 0 } }, options)) {
        fail('HA_QUOTA_POLICY', 'Legacy pool usage requires a verified all-window baseline before HA admission');
      }
      await profiles.insertOne({ _id: pool.id, allWindows: true, trackingVersion:1,startedAt: now }, options);
    } else if (profile.allWindows !== true||profile.trackingVersion!==1) fail('HA_QUOTA_POLICY', 'Incomplete or old pool usage baseline; verified cutover required');
    const reviewed=profile?.legacyReview;
    if(reviewed){
      if(reviewed.version!==1||!Number.isSafeInteger(reviewed.until)||!Number.isSafeInteger(reviewed.keyRowid)||
        ['keyIds','models','scheduleIds'].some(field=>!Array.isArray(reviewed[field])||reviewed[field].some(value=>typeof value!=='string')))
        fail('HA_QUOTA_POLICY','Invalid legacy pool review metadata');
      if(now<reviewed.until){
        if(!reviewed.models.includes(model))fail('HA_QUOTA_POLICY','Legacy pool model requires a reviewed historical baseline');
        if(!reviewed.keyIds.includes(keyId)){
          const key=await db.collection('api_keys').findOne({id:keyId},{...options,projection:{_ha_rowid:1}});
          if(!Number.isSafeInteger(key?._ha_rowid)||key._ha_rowid<=reviewed.keyRowid)fail('HA_QUOTA_POLICY','Pre-cutover key requires a reviewed pool baseline');
        }
      }
    }
    matched.push(pool.id);
    const override = await db.collection('provider_plans').findOne({ connection_id: pool.connection_id }, options);
    const plan = override ? json(override.dimensions_json) : getKnownPlan(provider)?.dimensions ?? [];
    if (!Array.isArray(plan) || !plan.length || plan.length > 32) fail('HA_RESERVATION_REQUIRED', 'Shared pool requires a configured countable plan');
    const all = await db.collection('quota_allocations').find({ pool_id: pool.id }, options).limit(1001).toArray();
    if (all.length > 1000 || all.some(a => !Number.isFinite(a.weight) || a.weight < 0 || a.weight > 100)) fail('HA_QUOTA_POLICY', 'Invalid pool allocation weights');
    const weight = all.reduce((sum, a) => sum + a.weight, 0) > 0 ? allocation.weight : 100 / all.length;
    const threshold = Number(process.env.QUOTA_SATURATION_THRESHOLD ?? 0.5);
    if (!Number.isFinite(threshold) || threshold < 0 || threshold > 1) fail('HA_QUOTA_POLICY', 'Invalid quota saturation threshold');
    if (allocation.cap_value > 0 && allocation.cap_unit === 'percent') fail('HA_RESERVATION_REQUIRED', 'Percentage allocations require a certified utilization bound');
    const held = await db.collection('_ha_reservations').aggregate([
      { $match: { poolIds: pool.id, state: { $in: ['reserved','dispatched','uncertain'] } } },
      { $group: { _id: '$keyId', tokens: { $sum: '$bound.tokens' }, usd: { $sum: '$bound.nanoUsd' }, requests: { $sum: 1 } } },
    ], options).toArray();
    const ownHeld = held.find(r => r._id === keyId);
    async function usage(namespace, unit, window, global) {
      if (!units.includes(unit) || !windows[window]) fail('HA_RESERVATION_REQUIRED', 'Unsupported pool dimension');
      const bucket = Math.floor(now / windows[window]);
      const field = unit === 'usd' ? {$cond:[{$eq:['$consumed',0]},0,'$_ha_usd_nano']} : '$consumed';
      const valid = {$cond:[{$isNumber:field},{$and:[{$gte:[field,0]},{$eq:[{$floor:field},field]}]},false]};
      const total = await db.collection('quota_consumption').aggregate([
        { $match: { dimension_key: `${namespace}:${unit}:${window}`, bucket_index: { $gte: bucket - 1 }, ...(global ? {} : { api_key_id: keyId }) } },
        { $group: { _id: null, n: { $sum: field }, min: { $min: '$consumed' }, invalid:{$sum:{$cond:[valid,0,1]}} } },
      ], options).next();
      if (total?.min < 0) fail('HA_QUOTA_UNCERTAIN', 'Negative pool consumption');
      if (unit==='usd' && total?.invalid) fail('HA_QUOTA_POLICY','USD pool usage requires verified integer accounting');
      // Whole adjacent buckets conservatively bound the original sliding window.
      return integer(total?.n ?? 0);
    }
    const info = [], consumedByThisKey = {};
    const seen = new Set();
    for (const dim of plan) {
      if (!units.includes(dim.unit) || !windows[dim.window] || !(dim.limit > Number.EPSILON)) fail('HA_RESERVATION_REQUIRED', 'Pool plan requires certified countable limits');
      const key = `${pool.id}:${dim.unit}:${dim.window}`;
      if (seen.has(key)) fail('HA_QUOTA_POLICY', 'Duplicate pool dimension');
      seen.add(key);
      const totalHeld = held.reduce((sum, h) => sum + integer(h[dim.unit]), 0), own = integer(ownHeld?.[dim.unit] ?? 0);
      const consumed = await usage(pool.id, dim.unit, dim.window, false) + own;
      const total = await usage(pool.id, dim.unit, dim.window, true) + totalHeld;
      const capacity = limit(dim.limit * ids.length, dim.unit), debit = amount(bound, dim.unit);
      if (total + debit > capacity) fail('HA_QUOTA_EXCEEDED', 'Shared pool has insufficient unreserved capacity');
      if (allocation.cap_value != null && allocation.cap_unit === dim.unit && consumed + debit > limit(allocation.cap_value, dim.unit)) fail('HA_QUOTA_EXCEEDED', 'Pool allocation absolute cap has insufficient capacity');
      if (threshold === 0 && !['soft','burst'].includes(allocation.policy) && consumed + debit > Math.floor(capacity * weight / 100)) fail('HA_QUOTA_EXCEEDED', 'Hard fair share has insufficient unreserved capacity');
      info.push({ key: { poolId: pool.id, unit: dim.unit, window: dim.window }, limit: capacity, consumedTotal: total, globalUsedPercent: 0 });
      consumedByThisKey[key] = consumed;
      dimensions.push({ poolId: pool.id, unit: dim.unit, window: dim.window });
    }
    const verdict = decideFairShare({ dimensions: info, allocation: { weight, capValue: allocation.cap_value == null ? undefined : limit(allocation.cap_value, allocation.cap_unit), capUnit: allocation.cap_unit,
      policy: allocation.policy }, consumedByThisKey, saturationThreshold: threshold });
    if (verdict.kind === 'block') fail('HA_QUOTA_EXCEEDED', 'Shared pool policy blocks this allocation');
    const cap = await db.collection('quota_allocation_model_caps').findOne({ pool_id: pool.id, api_key_id: keyId, model }, options);
    if (cap?.cap_value > Number.EPSILON) {
      const consumed = await usage(`${pool.id}:model:${model}`, cap.cap_unit, 'hourly', false);
      const modelHeld = await db.collection('_ha_reservations').aggregate([{ $match: { keyId, model, poolIds: pool.id, state: { $in: ['reserved','dispatched','uncertain'] } } },
        { $group: { _id: null, n: { $sum: cap.cap_unit === 'requests' ? 1 : cap.cap_unit === 'tokens' ? '$bound.tokens' : '$bound.nanoUsd' } } }], options).next();
      if (consumed + integer(modelHeld?.n ?? 0) + amount(bound, cap.cap_unit) > limit(cap.cap_value, cap.cap_unit)) fail('HA_QUOTA_EXCEEDED', 'Model allocation cap has insufficient capacity');
    }
    const rows = await db.collection('quota_schedules').find({ pool_id: pool.id, enabled: 1 }, options).limit(129).toArray();
    if (rows.length > 128) fail('HA_QUOTA_POLICY', 'Too many quota schedules');
    const normalized = rows.map(row => {
      const zone = row.timezone || process.env.TZ || 'UTC';
      try { new Intl.DateTimeFormat('en', { timeZone: zone }); } catch { fail('HA_QUOTA_POLICY', 'Invalid quota schedule timezone'); }
      const reserves = row.reserves ? json(row.reserves) : [];
      if (!Array.isArray(reserves) || reserves.length) fail('HA_RESERVATION_REQUIRED', 'Percentage reserve floors require an authoritative bounded utilization signal');
      return { id: row.id, poolId: row.pool_id, days: row.days.split(',').filter(p => p.trim() !== '').map(Number).filter(day => Number.isInteger(day) && day >= 0 && day <= 6), startMinute: row.start_minute, endMinute: row.end_minute,
        timezone: zone, mode: row.mode, reserves, budgetValue: row.budget_value, budgetUnit: row.budget_unit, budgetWindow: row.budget_window, priority: row.priority, enabled: true };
    });
    const schedule = resolveActiveSchedule(normalized, now);
    if(reviewed&&now<reviewed.until&&schedule.kind==='active'&&!reviewed.scheduleIds.includes(schedule.schedule.id))fail('HA_QUOTA_POLICY','Legacy pool schedule requires a reviewed historical baseline');
    if (schedule.kind === 'closed') fail('HA_QUOTA_EXCEEDED', 'Pool schedule is closed');
    if (schedule.kind === 'active') schedules.push(scheduleBucketPoolId(pool.id, schedule.schedule.id));
    if (schedule.kind === 'active' && schedule.schedule.budgetValue != null) {
      const s = schedule.schedule, namespace = scheduleBucketPoolId(pool.id, s.id);
      // A pending dispatch remains a liability even when the schedule closes.
      const pending = await db.collection('_ha_reservations').aggregate([{ $match: { scheduleIds: namespace, state: { $in: ['reserved','dispatched','uncertain'] } } },
        { $group: { _id: null, n: { $sum: s.budgetUnit === 'requests' ? 1 : s.budgetUnit === 'tokens' ? '$bound.tokens' : '$bound.nanoUsd' } } }], options).next();
      const consumed = await usage(namespace, s.budgetUnit, s.budgetWindow, true);
      if (consumed + integer(pending?.n ?? 0) + amount(bound, s.budgetUnit) > limit(s.budgetValue, s.budgetUnit)) fail('HA_QUOTA_EXCEEDED', 'Schedule budget has insufficient capacity');
    }
  }
  const namespaces=[...new Set([...matched,...matched.map(pool=>`${pool}:model:${model}`),...schedules])];
  if (namespaces.length) {
    const filters=usdFilters(keyId,namespaces,now);
    const rows=await db.collection('quota_consumption').find({$or:filters},{session}).limit(filters.length+1).toArray();
    if (rows.length>filters.length) fail('HA_QUOTA_POLICY','Duplicate USD pool counters');
    for (const row of rows) integer(exactUsd(row)+bound.nanoUsd);
  }
  return { poolIds: matched, scheduleIds: schedules, poolDimensions: dimensions };
}

export async function settlePools(db, session, row, receipt, now) {
  // Maintain every countable window, not just currently enabled plan dimensions:
  // adding/changing a cap must retain this deployment's already consumed usage.
  const namespaces = [...new Set([...(row.poolIds ?? []), ...(row.poolIds ?? []).map(pool => `${pool}:model:${row.model}`), ...(row.scheduleIds ?? [])])];
  if (!namespaces.length) return;
  await consumptionSchema(db, session);
  const collection = db.collection('quota_consumption'), updates = [];
  const largest = await collection.findOne({}, { session, sort: { _ha_rowid: -1 }, projection: { _ha_rowid: 1 } });
  let rowid = Math.max(0, largest?._ha_rowid ?? 0);
  integer(rowid + namespaces.length * 15);
  for (const poolId of namespaces) for (const [window, ms] of Object.entries(windows)) for (const unit of units) {
    const value = unit === 'requests' ? 1 : unit === 'tokens' ? receipt.tokens : receipt.nanoUsd / 1e9;
    const filter = { api_key_id: row.keyId, dimension_key: `${poolId}:${unit}:${window}`, bucket_index: Math.floor(now / ms) };
    updates.push({ updateOne: { filter, update: { $inc: { consumed: value,...(unit==='usd'?{_ha_usd_nano:receipt.nanoUsd}:{}) }, $set: { updated_at: now }, $setOnInsert: { _ha_rowid: ++rowid } }, upsert: true } });
  }
  // The global writer fence serializes this bulk with SQL-facing writes. Check
  // the pinned schema, existing PK uniqueness and values before using native
  // increments; retain stable rowids and the ordinary SQL-visible columns.
  const seen = new Set();
  const existing = await collection.find({ $or: updates.map(u => u.updateOne.filter) }, { session }).limit(updates.length + 1).toArray();
  for (const row of existing) {
    const key = JSON.stringify([row.api_key_id, row.dimension_key, row.bucket_index]);
    if (seen.has(key) || !Number.isFinite(row.consumed) || row.consumed < 0 || !Number.isSafeInteger(row._ha_rowid)) fail('HA_QUOTA_UNCERTAIN', 'Invalid persisted pool counter');
    if (/:usd:(hourly|5h|daily|weekly|monthly)$/.test(row.dimension_key)) integer(exactUsd(row)+receipt.nanoUsd);
    seen.add(key);
  }
  await collection.bulkWrite(updates, { session, ordered: true });
}
