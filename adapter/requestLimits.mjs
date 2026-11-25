import { activeTeam } from './teamQuota.mjs';
// Called inside a fenced transaction, including the early compatibility probe.
export async function checkQuotaPolicy(db, session, keyId, reservations = false) {
  if (typeof keyId !== 'string' || !keyId) throw new TypeError('Invalid quota key');
  const policies = [];
  // MongoDB session transactions do not support parallel operations.
  for (const [collection, filter] of [
    ['api_key_quota_limits', { api_key_id: keyId }], ['domain_budgets', { api_key_id: keyId }],
    ['api_keys', { id: keyId }], ['api_key_token_limits', { api_key_id: keyId }],
    ['quota_allocations', { api_key_id: keyId }],
  ]) policies.push(await db.collection(collection).findOne(filter, { session }));
  const [quota, budget, key, tokenLimit, allocation] = policies;
  const team = await activeTeam(db,session,keyId);
  const keyUsage=key?.usage_limit_enabled && (key.daily_usage_limit_usd>0 || key.weekly_usage_limit_usd>0);
  if (!keyUsage) await db.collection('_ha_key_usage_profiles').deleteOne({_id:keyId},{session});
  // Threshold checks plus POST-response counters cannot enforce an upper bound
  // under concurrency or a crash. Reject unsupported hard policies explicitly;
  // never let the upstream fail-open catches masquerade as reservation support.
  const keyManaged=!!(quota?.tpm_limit > 0 || quota?.monthly_amount_usd > 0 || tokenLimit || keyUsage ||
    ['daily_limit_usd', 'weekly_limit_usd', 'monthly_limit_usd'].some(field => budget?.[field] > 0));
  if(keyManaged&&key?._ha_requires_key_baseline===true)throw Object.assign(new Error('Imported key token/USD history requires verified offline baseline'),{code:'HA_QUOTA_POLICY'});
  const managed=!!(keyManaged||team?.max_budget_usd>0||allocation);
  if (managed && !reservations) {
    throw Object.assign(new Error('Hard token/USD or shared-pool quotas require durable reservations, unavailable in this HA profile'), { code: 'HA_RESERVATION_REQUIRED' });
  }
  const rpm = quota?.rpm_limit > 0 ? quota.rpm_limit : null;
  if (rpm != null && !Number.isSafeInteger(rpm)) throw new TypeError('Invalid RPM quota');
  return { rpm, managed };
}

// One document per key: live fixed windows and two RPM buckets. Check every
// applicable limit before publishing any consumption, in one fenced transaction.
export async function admitRequest(db, session, keyId, rules, reservations = false) {
  if (!Array.isArray(rules) || rules.length > 128) throw new TypeError('Invalid request-limit policy');
  const limits = new Map();
  for (const { window, limit } of rules) {
    if (!Number.isSafeInteger(window) || window < 1 || window > 31536000 ||
      !Number.isSafeInteger(limit) || limit < 0) throw new TypeError('Invalid request-limit window or count');
    limits.set(window, Math.min(limits.get(window) ?? Infinity, limit));
  }
  const { rpm } = await checkQuotaPolicy(db, session, keyId, reservations);
  const [{ now }] = await db.aggregate([{ $documents: [{}] }, { $project: { now: { $toLong: '$$NOW' } } }], { session }).toArray();
  const counters = db.collection('_ha_request_limits');
  const previous = await counters.findOne({ _id: keyId }, { session });
  const live = (previous?.windows ?? []).filter(w => w.resetAt > now);
  const bucket = Math.floor(now / 60000);
  const rpmBuckets = (previous?.rpm ?? []).filter(w => w.bucket >= bucket - 1 && w.bucket <= bucket);
  if (!previous?.rpmInitialized) {
    // Seed pre-adapter history once. Thereafter admission, not completion,
    // owns RPM. Re-merging completion buckets would double-charge a request
    // that spans a minute boundary. Track admissions even while RPM is disabled
    // so enabling a policy cannot forget traffic already admitted by this app.
    const recorded = await db.collection('api_key_quota_counters').find({ api_key_id: keyId,
      dimension_key: 'key-quota:rpm', bucket_index: { $gte: bucket - 1, $lte: bucket } }, { session }).toArray();
    for (const row of recorded) {
      if (!Number.isSafeInteger(row.consumed) || row.consumed < 0) throw new TypeError('Invalid persisted RPM counter');
      const existing = rpmBuckets.find(w => w.bucket === row.bucket_index);
      if (existing) existing.count = Math.max(existing.count, row.consumed);
      else rpmBuckets.push({ bucket: row.bucket_index, count: row.consumed });
    }
  }
  const currentRpm = rpmBuckets.find(w => w.bucket === bucket);
  const priorRpm = rpmBuckets.find(w => w.bucket === bucket - 1);
  const used = (currentRpm?.count ?? 0) + (priorRpm?.count ?? 0) * (1 - now % 60000 / 60000);
  if (rpm != null && used + 1 > rpm) return { allowed: false, failedWindow: 60, resetAt: (bucket + 1) * 60000 };
  if (currentRpm) currentRpm.count++;
  else rpmBuckets.push({ bucket, count: 1 });
  for (const [window, limit] of limits) {
    const current = live.find(w => w.window === window);
    const resetAt = (Math.floor(now / (window * 1000)) + 1) * window * 1000;
    if ((current?.count ?? 0) >= limit) return { allowed: false, failedWindow: window, resetAt };
    if (current) current.count++;
    else live.push({ window, count: 1, resetAt });
  }
  const expires = Math.max(...live.map(w => w.resetAt), ...(rpmBuckets.length ? [(bucket + 2) * 60000] : []));
  await counters.replaceOne({ _id: keyId }, { _id: keyId, windows: live, rpm: rpmBuckets, rpmInitialized: true, expiresAt: new Date(expires) }, { upsert: true, session });
  return { allowed: true };
}
