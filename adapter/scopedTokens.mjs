import { getBudgetWindow } from './budgetCalendar.mjs';
const fail = message => { throw Object.assign(new Error(message), { code: 'HA_QUOTA_POLICY' }); };
const matches = (limit, request) => limit.scope_type === 'global' ||
  limit.scope_type === 'provider' && limit.scope_value === request.provider || limit.scope_type === 'model' && limit.scope_value === request.model;
export const scopedTokenSpecification=row=>JSON.stringify([row._ha_rowid,row.api_key_id,row.scope_type,row.scope_value,row.reset_interval,row.reset_time]);
async function policies(db, session, request, now, schema) {
  const options = { session }, rows = await db.collection('api_key_token_limits').find({ api_key_id: request.keyId }, options).limit(129).toArray();
  if (rows.length > 128) fail('Too many scoped token policies');
  if (rows.length) { await schema('api_key_token_counters'); await schema('api_key_token_limit_reset_logs'); }
  for (const row of rows) {
    if (typeof row.id !== 'string' || !row.id || typeof row.scope_value !== 'string' ||
        row.scope_type !== 'global' && !row.scope_value || row.reset_time != null && typeof row.reset_time !== 'string' ||
        !['global','provider','model'].includes(row.scope_type) || !['daily','weekly','monthly'].includes(row.reset_interval) ||
        !Number.isSafeInteger(row.token_limit) || row.token_limit < 1 || !Number.isSafeInteger(row._ha_rowid) || ![0,1].includes(row.enabled)) fail('Invalid scoped token policy');
    const spec = scopedTokenSpecification(row);
    const profiles = db.collection('_ha_scoped_token_profiles'), profile = await profiles.findOne({ _id: row.id }, options);
    const { periodStartAt } = getBudgetWindow(row.reset_interval,row.reset_time || '00:00',now);
    row.windowStart = String(periodStartAt);
    row.counter = await db.collection('api_key_token_counters').findOne({ limit_id: row.id, window_start: row.windowStart }, options);
    if (row.counter && (!Number.isSafeInteger(row.counter.tokens_used) || row.counter.tokens_used < 0)) fail('Invalid scoped token counter');
    if (profile?.spec !== spec) {
      // No guessed join/deduplication of historical analytics and billing rows.
      // Fresh tracking starts only in a provably empty applicable window.
      const filter = { api_key_id: request.keyId, timestamp: { $gte: new Date(periodStartAt).toISOString() },
        ...(row.scope_type === 'global' ? {} : { [row.scope_type]: row.scope_value }),
        $expr: { $gt: [{ $add: [{ $ifNull: ['$tokens_input',0] },{ $ifNull: ['$tokens_output',0] },{ $ifNull: ['$tokens_reasoning',0] }] },0] } };
      if (row.counter?.tokens_used > 0 || await db.collection('usage_history').findOne(filter,options) || await db.collection('_ha_usage_history').findOne(filter,options) || await db.collection('request_cost_ledger').findOne(filter,options)) {
        fail('Scoped token policy requires a verified historical baseline or an empty applicable window');
      }
      await profiles.replaceOne({ _id: row.id }, { _id: row.id, spec, startedAt: now, allWindows: true }, { ...options, upsert: true });
    }
  }
  return rows;
}
export async function checkScopedTokens(db, session, request, now, schema) {
  for (const row of await policies(db,session,request,now,schema)) {
    if (!row.enabled || !matches(row,request)) continue;
    const held = await db.collection('_ha_reservations').aggregate([
      { $match: { keyId: request.keyId, state: { $in: ['reserved','dispatched','uncertain'] },
        ...(row.scope_type === 'global' ? {} : { [row.scope_type]: row.scope_value }) } },
      { $group: { _id: null, n: { $sum: '$bound.tokens' } } },
    ],{session}).next();
    const projected = (row.counter?.tokens_used ?? 0) + (held?.n ?? 0) + request.bound.tokens;
    if (!Number.isSafeInteger(projected) || projected > row.token_limit) throw Object.assign(new Error('Hard scoped token quota has insufficient unreserved capacity'), {code:'HA_QUOTA_EXCEEDED'});
  }
}
export async function settleScopedTokens(db,session,request,receipt,now,schema,execute) {
  for (const row of await policies(db,session,request,now,schema)) {
    // Keep tracking disabled rows so reenabling cannot discard intervening usage.
    if (!matches(row,request)) continue;
    if (!Number.isSafeInteger((row.counter?.tokens_used ?? 0) + receipt.tokens)) fail('Scoped token counter overflow');
    const run = (sql,args) => execute({sql,args,mode:'run'});
    if (!row.counter) {
      const prior = await db.collection('api_key_token_counters').find({limit_id:row.id,window_start:{$lt:row.windowStart}},{session}).sort({window_start:-1}).limit(1).next();
      if (prior?.tokens_used > 0) await run('INSERT INTO api_key_token_limit_reset_logs(limit_id,prev_tokens,window_start) VALUES (?,?,?)',[row.id,prior.tokens_used,row.windowStart]);
    }
    await run('INSERT INTO api_key_token_counters(limit_id,window_start,tokens_used) VALUES (?,?,?) ON CONFLICT(limit_id,window_start) DO UPDATE SET tokens_used=tokens_used+excluded.tokens_used,updated_at=CURRENT_TIMESTAMP',
      [row.id,row.windowStart,receipt.tokens]);
  }
}
