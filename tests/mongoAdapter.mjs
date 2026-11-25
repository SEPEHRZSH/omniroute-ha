import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import { MongoAdapter } from '../adapter/mongoAdapter.mjs';
import { testDatabase } from './mongoTestSupport.mjs';
import { conversationCandidatesSql } from '../adapter/conversationQuery.mjs';
import { latestQuotaSql } from '../adapter/quotaQuery.mjs';
import { usageSpendSql,codexDisplaySql,teamSpendSql } from '../adapter/usageQueries.mjs';
import { providerRecoverySql } from '../adapter/providerQueries.mjs';
import {auditPruneSql,reasoningRetentionSql} from '../adapter/retentionQueries.mjs';

const test = await testDatabase('adapter');
const oracle = new DatabaseSync(':memory:'); // Test oracle only. Never an application fallback.
let db, peer;
try {
  db = new MongoAdapter({ uri: test.uri, database: test.database });
  peer = new MongoAdapter({ uri: test.uri, database: test.database });
  const ddl = 'CREATE TABLE key_value (namespace TEXT NOT NULL, key TEXT NOT NULL, value TEXT, PRIMARY KEY (namespace, key))';
  for (const target of [db, oracle]) target.exec(ddl);
  const lkgpPrefix = "SELECT key FROM key_value WHERE namespace = 'lkgp' AND key LIKE ? ESCAPE '\\'";
  for (const target of [db, oracle]) {
    const insert = target.prepare('INSERT INTO key_value (namespace, key) VALUES (?, ?)');
    for (const key of ['Combo_name:one', 'comboXname:two', 'combo%name:three', 'combo\\name:four', 'combo:name\n']) {
      insert.run('lkgp', key);
    }
    insert.run('unrelated', 'combo_name:five');
  }
  for (const pattern of ['combo\\_name:%', 'combo\\%name:%', 'combo\\\\name:%', 'combo:name', 'combo\\', null]) {
    assert.deepEqual(db.prepare(lkgpPrefix).all(pattern).map(row => row.key),
      oracle.prepare(lkgpPrefix).all(pattern).map(row => row.key));
  }
  for (const target of [db, oracle]) target.exec("DELETE FROM key_value WHERE namespace IN ('lkgp', 'unrelated')");
  console.log('PASS original Combo cleanup lookup vs SQLite: escaped wildcards and backslashes, ASCII folding, exact end and namespace isolation');
  for(const target of [db,oracle]) {
    target.exec('CREATE TABLE audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT,timestamp TEXT NOT NULL,action TEXT NOT NULL)');
    for(const [timestamp,action] of [['2026-01-01','touch'],['2026-01-02','touch'],['2026-01-02','touch'],['2026-01-03','other'],['2026-01-03','touch']])
      target.prepare('INSERT INTO audit_log(timestamp,action) VALUES (?,?)').run(timestamp,action);
  }
  const auditRows=target=>target.prepare('SELECT * FROM audit_log ORDER BY id').all().map(row=>({...row}));
  const beforeAudit=auditRows(db);
  assert.throws(()=>db.transaction(()=>{assert.equal(db.prepare(auditPruneSql).run('touch',2).changes,2);throw new Error('rollback audit retention');})(),/rollback audit retention/);
  assert.deepEqual(auditRows(db),beforeAudit);
  for(const [action,offset] of [['touch',9],[null,0],['touch',2],['other',0],['touch',0]]) {
    assert.equal(db.prepare(auditPruneSql).run(action,offset).changes,oracle.prepare(auditPruneSql).run(action,offset).changes);
    assert.deepEqual(auditRows(db),auditRows(oracle));
  }
  console.log('PASS exact original audit retention vs SQLite: timestamp/id ties, offset boundaries, NULL/action isolation, zero retention and transactional rollback');
  for(const target of [db,oracle]) {
    target.exec(readFileSync('upstream/omniroute/src/lib/db/migrations/033_create_reasoning_cache.sql','utf8'));
    for(const [id,provider,model,expiry] of [['old','a','m',1],['zero','b','n',0],['fraction','a','m',1.9],['future','a','m',3000000000],['future-fraction','b','n',3000000000.9]])
      target.prepare('INSERT INTO reasoning_cache(tool_call_id,provider,model,reasoning,char_count,created_at,expires_at) VALUES (?,?,?,?,?,?,?)').run(id,provider,model,'Synthetic reasoning '+id,10,'2026-01-01',expiry);
  }
  const expiryDelete=reasoningRetentionSql.find(sql=>sql.startsWith('DELETE'));
  assert.throws(()=>db.transaction(()=>{assert.equal(db.prepare(expiryDelete).run().changes,3);throw new Error('rollback reasoning expiry');})(),/rollback reasoning expiry/);
  assert.equal(db.prepare('SELECT COUNT(*) AS count FROM reasoning_cache').get().count,5);
  for(const sql of reasoningRetentionSql) {
    const args=sql.includes('WHERE tool_call_id = ?')?['future']:[];
    if(sql.includes('AND provider = ?'))args.push('a');if(sql.includes('AND model = ?'))args.push('m');
    if(sql.includes('LIMIT ? OFFSET ?'))args.push(2,0);
    if(sql.startsWith('DELETE'))assert.equal(db.prepare(sql).run(...args).changes,oracle.prepare(sql).run(...args).changes);
    else assert.deepEqual(db.prepare(sql).all(...args).map(row=>({...row})),oracle.prepare(sql).all(...args).map(row=>({...row})));
  }
  console.log('PASS all exact original reasoning expiry queries vs SQLite: numeric INTEGER-affinity epochs, REAL truncation, stats/grouping, read/filter/pagination, cleanup and rollback; legacy text dates are not certified');
  await test.db.collection('reasoning_cache').insertOne({_id:'synthetic-legacy-expiry',tool_call_id:'legacy-date',provider:'a',model:'m',reasoning:'Synthetic legacy storage',char_count:1,created_at:'2026-01-01',expires_at:'2099-01-01T00:00:00Z'});
  const legacyReasoning=new MongoAdapter({uri:test.uri,database:test.database});
  try{
    assert.throws(()=>legacyReasoning.prepare(expiryDelete).run(),{code:'HA_UNSUPPORTED_SQL'});
    assert.ok(await test.db.collection('reasoning_cache').findOne({_id:'synthetic-legacy-expiry'}),'Uncertified legacy expiry must refuse and retain the row, not classify it as expired');
  }finally{await legacyReasoning.close();await test.db.collection('reasoning_cache').deleteOne({_id:'synthetic-legacy-expiry'});}
  for (const target of [db,oracle]) {
    target.exec('CREATE TABLE usage_history(api_key_id TEXT,timestamp TEXT,success INTEGER,provider TEXT,model TEXT,service_tier TEXT,provider_cost_usd REAL,tokens_input INTEGER,tokens_output INTEGER,tokens_cache_read INTEGER,tokens_cache_creation INTEGER,tokens_reasoning INTEGER)');
    for (const row of [
      ['a','2026-01-02',1,'Provider','Model',null,null,10,5,2,3,1],['a','2026-01-02',1,'provider','model','',null,2,1,0,0,0],
      ['a','2026-01-02',0,'provider','model','standard',0.25,100,100,0,0,0],['a','2026-01-02',0,'provider','model','standard',null,999,999,0,0,0],
      ['a','2026-01-02',1,'ÖZ','model','priority',0,0,0,0,0,0],['b','2026-01-02',1,'provider','model','standard',null,999,999,0,0,0],
      ['a','2025-01-02',1,'provider','model','standard',null,999,999,0,0,0],['a','2026-01-02',1,null,'model',null,null,null,null,null,null,null],
    ]) target.prepare('INSERT INTO usage_history(api_key_id,timestamp,success,provider,model,service_tier,provider_cost_usd,tokens_input,tokens_output,tokens_cache_read,tokens_cache_creation,tokens_reasoning) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)').run(...row);
    target.exec('CREATE TABLE provider_connections(id TEXT PRIMARY KEY,provider TEXT,provider_specific_data TEXT)');
    for (const [index,value] of [null,'invalid','{"codexQuotaState": { "usage7d": 1.0, "text":"x y" }}','{"codexQuotaState":{"a":1},"codexQuotaState":{"a":2}}','{"codexQuotaStateByScope":{"codex":{"usage7d":2}},"secret":"synthetic-never-return"}'].entries()) target.prepare('INSERT INTO provider_connections(id,provider,provider_specific_data) VALUES (?,?,?)').run(String(index),'codex',value);
  }
  const sorted = rows => rows.map(row=>Object.fromEntries(Object.entries(row).sort(([a],[b])=>a.localeCompare(b)))).sort((a,b)=>JSON.stringify(a).localeCompare(JSON.stringify(b)));
  assert.deepEqual(sorted(db.prepare(usageSpendSql).all({apiKeyId:'a',sinceIso:'2026'})),sorted(oracle.prepare(usageSpendSql).all({apiKeyId:'a',sinceIso:'2026'})));
  assert.deepEqual(sorted(db.prepare(codexDisplaySql).all()),sorted(oracle.prepare(codexDisplaySql).all()));
  assert.deepEqual(db.prepare(codexDisplaySql+' AND id = ?').get('3'),{...oracle.prepare(codexDisplaySql+' AND id = ?').get('3')});
  assert.ok(!JSON.stringify(db.prepare(codexDisplaySql).all()).includes('synthetic-never-return'));
  for(const target of [db,oracle]) {
    target.exec('ALTER TABLE usage_history ADD COLUMN billing_team_id TEXT; ALTER TABLE usage_history ADD COLUMN team_rollup_processed_at TEXT');
    target.prepare('UPDATE usage_history SET billing_team_id=? WHERE api_key_id=?').run('team','a');
    target.exec('CREATE TABLE daily_team_usage_summary(team_id TEXT,api_key_id TEXT,provider TEXT,model TEXT,service_tier TEXT,date TEXT,successful_requests INTEGER,successful_input_tokens INTEGER,successful_output_tokens INTEGER,successful_cache_read_tokens INTEGER,successful_cache_creation_tokens INTEGER,successful_reasoning_tokens INTEGER,successful_token_shapes_json TEXT)');
    for(const row of [['team','a','Provider','Model',null,'2026-01-02',2,10,5,2,3,1,'[]'],['team','a','provider','model',null,'2026-01-02',1,2,1,0,0,0,null],['other','a','provider','model','standard','2026-01-02',9,900,900,0,0,0,null]])
      target.prepare('INSERT INTO daily_team_usage_summary(team_id,api_key_id,provider,model,service_tier,date,successful_requests,successful_input_tokens,successful_output_tokens,successful_cache_read_tokens,successful_cache_creation_tokens,successful_reasoning_tokens,successful_token_shapes_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)').run(...row);
  }
  for(const [index,sql] of teamSpendSql.entries()) {
    const parameters=index===0?{teamId:'team',windowStartIso:'2026',resetAtIso:'2027'}:{teamId:'team',completeSummaryStartDate:'2026',completeSummaryEndDateExclusive:'2027'};
    assert.deepEqual(sorted(db.prepare(sql).all(parameters)),sorted(oracle.prepare(sql).all(parameters)));
  }
  for(const args of [['team','2026-01-02'],['team','missing']]) {
    const exists='SELECT 1 FROM daily_team_usage_summary WHERE team_id = ? AND date = ? AND successful_requests > 0 LIMIT 1';
    assert.deepEqual(db.prepare(exists).get(...args),oracle.prepare(exists).get(...args)?{...oracle.prepare(exists).get(...args)}:undefined);
  }
  console.log('PASS original Team usage SQL vs SQLite: raw token-shape grouping, successful-only rows, retained daily summaries, null/case/service tiers and team/window isolation');
  for(const target of [db,oracle]) {
    for(const name of ['test_status','last_error','last_error_at','last_error_type','last_error_source','error_code','rate_limited_until','updated_at']) target.exec(`ALTER TABLE provider_connections ADD COLUMN ${name} TEXT`);
    target.exec('ALTER TABLE provider_connections ADD COLUMN backoff_level INTEGER');
    for(const [index,json] of [null,'invalid','{"keep":1.0,"codexScopeRateLimitedUntil":{},"codexScopeRateLimitSource":{}}','{"codexScopeRateLimitedUntil":1,"codexScopeRateLimitedUntil":2,"keep":9007199254740993}','[]','null','3','"text"'].entries())
      target.prepare('INSERT INTO provider_connections(id,provider,provider_specific_data,test_status,last_error_at,rate_limited_until) VALUES (?,?,?,?,?,?)').run('recovery-'+index,'codex',json,'unavailable','old','old');
  }
  for(let index=0;index<8;index++) {
    const args=['new','recovery-'+index,'unavailable','old','old'];
    assert.equal(db.prepare(providerRecoverySql).run(...args).changes,oracle.prepare(providerRecoverySql).run(...args).changes);
    const select='SELECT provider_specific_data,test_status,last_error_at,rate_limited_until,updated_at FROM provider_connections WHERE id=?';
    assert.deepEqual(db.prepare(select).get(args[1]),{...oracle.prepare(select).get(args[1])});
    assert.equal(db.prepare(providerRecoverySql).run(...args).changes,0,'Stale CAS must not clear newer state');
  }
  console.log('PASS original provider recovery CAS vs SQLite: conditional update, malformed/scalar/object/array JSON, duplicate keys and exact numeric lexemes');
  // Simulate malformed legacy/native BSON; the SQL facade already refuses
  // writing a BLOB into this TEXT column, independently of this read boundary.
  await test.db.collection('provider_connections').insertOne({id:'binary-quota',provider:'codex',provider_specific_data:Buffer.from('{"codexQuotaState":{}}'),_ha_rowid:99999});
  const invalidProjection=new MongoAdapter({uri:test.uri,database:test.database});
  try {assert.throws(()=>invalidProjection.prepare(codexDisplaySql+' AND id = ?').get('binary-quota'),{code:'HA_UNSUPPORTED_SQL'});}
  finally {invalidProjection.close();}
  console.log('PASS actual usage SQL vs SQLite: priced/unpriced aggregates, failed billable calls, ASCII case/null/service tiers, credential-free Codex JSON projection, duplicate keys and preserved numeric lexemes');
  // Fail the metadata write after collection creation to verify atomic rollback.
  await test.db.command({ collMod: '_ha_schema', validator: { _id: { $ne: 'rejected_schema' } } });
  assert.throws(() => db.exec('CREATE TABLE rejected_schema (id TEXT PRIMARY KEY)'), { code: 121 });
  assert.equal(await test.db.collection('_ha_schema').findOne({ _id: 'rejected_schema' }), null);
  assert.equal((await test.db.listCollections({ name: 'rejected_schema' }).toArray()).length, 0);
  await test.db.command({ collMod: '_ha_schema', validator: {} });
  db.exec('CREATE TABLE rejected_schema (id TEXT PRIMARY KEY)');
  assert.ok(await test.db.collection('_ha_schema').findOne({ _id: 'rejected_schema' }));
  for (const table of ['key_value', 'nonexistent', 'memory_fts']) {
    const sql = "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?";
    assert.deepEqual(db.prepare(sql).all(table), oracle.prepare(sql).all(table).map(row => ({ ...row })));
  }
  const insert = "INSERT OR REPLACE INTO key_value(namespace, key, value) VALUES ('settings', ?, ?)";
  const committed = [];
  db.onCommitted = tables => committed.push({tables,inTransaction:db.inTransaction,value:db.prepare("SELECT value FROM key_value WHERE namespace='settings' AND key='notification-a'").get().value});
  db.transaction(() => { db.prepare(insert).run('notification-a', 'a'); db.prepare(insert).run('notification-b', 'b'); })();
  assert.deepEqual(committed, [{tables:['key_value'],inTransaction:false,value:'a'}],'Commit observers synchronously see committed data outside the ended transaction');
  assert.throws(() => db.transaction(() => { db.prepare(insert).run('rolled-back-notification', 'x'); throw new Error('rollback hint'); })(), /rollback hint/);
  await new Promise(resolve => setImmediate(resolve)); assert.equal(committed.length, 1);
  db.onCommitted = undefined;
  db.prepare("DELETE FROM key_value WHERE key IN ('notification-a','notification-b')").run();
  const payload = "a'; DELETE FROM key_value; -- ? @secret";
  for (const target of [db, oracle]) {
    target.prepare(insert).run('theme', payload);
    target.prepare(insert).run('nullValue', null);
    target.prepare(insert).run('theme', 'dark');
  }
  assert.deepEqual(db.prepare(insert).run('theme', 'dark'), oracle.prepare(insert).run('theme', 'dark'));
  for (const query of [
    "SELECT key,value FROM key_value WHERE namespace='settings' ORDER BY key",
    "SELECT key FROM key_value WHERE value <> 'dark'",
    "SELECT key FROM key_value WHERE value IS NULL OR value = 'dark' ORDER BY key",
    "SELECT key FROM key_value WHERE value IS NULL AND key = 'nullValue'",
    "SELECT key FROM key_value WHERE value IN ('dark', NULL) ORDER BY key",
    "SELECT key FROM key_value WHERE value NOT IN ('dark', NULL) ORDER BY key",
  ]) assert.deepEqual(db.prepare(query).all(), oracle.prepare(query).all().map(row => ({ ...row })), query);
  assert.deepEqual(db.prepare('SELECT value FROM key_value WHERE namespace=@ns AND key=?').get({ ns: 'settings' }, 'theme'), { value: 'dark' });
  assert.deepEqual(db.prepare('SELECT value FROM key_value WHERE namespace=? /* ignored ? */ AND key=? -- @ignored').get('settings', 'theme'), { value: 'dark' });
  assert.throws(() => db.prepare(insert).run('missing'), { code: 'SQLITE_RANGE' });
  assert.equal(db.prepare('SELECT key FROM key_value').all().length, 2);
  assert.throws(() => db.prepare('INSERT INTO key_value(namespace,key) VALUES (?,?)').run('settings', 'theme'), { code: 'SQLITE_CONSTRAINT_UNIQUE' });
  assert.throws(() => db.prepare('INSERT INTO key_value(namespace,key) VALUES (?,?)').run(null, 'bad'), { code: 'SQLITE_CONSTRAINT_NOTNULL' });
  assert.throws(() => db.transaction(() => {
    db.prepare(insert).run('rolledBack', 'no');
    throw new Error('abort caller');
  })(), /abort caller/);
  assert.equal(db.prepare("SELECT key FROM key_value WHERE key='rolledBack'").get(), undefined);
  db.exec('BEGIN IMMEDIATE');
  db.prepare(insert).run('pending', 'hidden');
  assert.equal(peer.prepare("SELECT value FROM key_value WHERE key='pending'").get(), undefined);
  assert.throws(() => peer.prepare(insert).run('contender', 'no'), { code: 'SQLITE_BUSY' });
  db.exec('COMMIT');
  assert.deepEqual(peer.prepare("SELECT value FROM key_value WHERE key='pending'").get(), { value: 'hidden' });
  assert.equal(await test.db.collection('key_value').countDocuments({ key: 'contender' }), 0);
  assert.equal(await test.db.collection('key_value').countDocuments({ key: 'pending' }), 1);
  db.exec('CREATE TABLE blobs (id TEXT PRIMARY KEY, payload BLOB)');
  db.prepare('INSERT INTO blobs(id,payload) VALUES (?,?)').run('binary', Buffer.from([0, 255, 1]));
  assert.deepEqual(db.prepare('SELECT payload FROM blobs WHERE id=?').get('binary').payload, Buffer.from([0, 255, 1]));
  for (const target of [db, oracle]) {
    target.exec('CREATE TABLE text_binding_affinity(id TEXT PRIMARY KEY,value TEXT)');
    for (const [index, value] of [503, 429, -0, -1, 10000000000000].entries()) {
      target.prepare('INSERT INTO text_binding_affinity(id,value) VALUES (?,?)').run(String(index), value);
    }
    target.prepare('UPDATE text_binding_affinity SET value=@status WHERE id=@id').run({ status: 502, id: '0' });
  }
  assert.deepEqual(db.prepare('SELECT * FROM text_binding_affinity ORDER BY id').all(), oracle.prepare('SELECT * FROM text_binding_affinity ORDER BY id').all().map(row => ({ ...row })));
  for (const target of [db,oracle]) {
    target.exec('CREATE TABLE case_order(id INTEGER PRIMARY KEY,scope_type TEXT,scope_value TEXT)');
    for (const [index,value] of ['global','model','provider',null,'Model','$model','model'].entries()) target.prepare('INSERT INTO case_order(id,scope_type,scope_value) VALUES (?,?,?)').run(index,value,String(10-index));
  }
  for (const direction of ['ASC','DESC']) {
    const query = "SELECT * FROM case_order ORDER BY CASE scope_type WHEN 'model' THEN 0 WHEN 'provider' THEN 1 ELSE 2 END "+direction+",scope_value";
    assert.deepEqual(db.prepare(query).all(),oracle.prepare(query).all().map(row=>({...row})),query);
  }
  for (const target of [db, oracle]) target.exec('CREATE TABLE sequence_test (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT UNIQUE)');
  for (const [sql, args] of [
    ['INSERT INTO sequence_test(value) VALUES (?)', ['one']],
    ['INSERT OR IGNORE INTO sequence_test(value) VALUES (?)', ['one']],
    ['INSERT INTO sequence_test(value) VALUES (?)', ['two']],
    ['INSERT INTO sequence_test(id,value) VALUES (?,?)', [50, 'fifty']],
    ['DELETE FROM sequence_test WHERE id=?', [50]],
    ['INSERT INTO sequence_test(value) VALUES (?)', ['after-delete']],
  ]) assert.deepEqual(db.prepare(sql).run(...args), oracle.prepare(sql).run(...args), sql);
  assert.deepEqual(db.prepare('SELECT * FROM sequence_test ORDER BY id').all(), oracle.prepare('SELECT * FROM sequence_test ORDER BY id').all().map(row => ({ ...row })));
  assert.throws(() => db.transaction(() => { db.prepare('INSERT INTO sequence_test(value) VALUES (?)').run('rollback'); throw new Error('rollback sequence'); })(), /rollback sequence/);
  oracle.exec("BEGIN; INSERT INTO sequence_test(value) VALUES ('rollback'); ROLLBACK");
  const sequence = await test.db.collection('_ha_sequences').findOne({ _id: 'sequence_test' });
  assert.equal(sequence.value, 51);
  const scalarUpdate = 'UPDATE sequence_test SET value=? WHERE id=(SELECT id FROM sequence_test WHERE value LIKE ? ORDER BY id DESC LIMIT 1)';
  for (const args of [['latest','%'], ['no-match','absent']]) {
    assert.deepEqual(db.prepare(scalarUpdate).run(...args), oracle.prepare(scalarUpdate).run(...args));
  }
  assert.deepEqual(db.prepare('SELECT * FROM sequence_test ORDER BY id').all(), oracle.prepare('SELECT * FROM sequence_test ORDER BY id').all().map(row => ({ ...row })));
  db.exec("CREATE TABLE date_defaults (id INTEGER PRIMARY KEY, created TEXT NOT NULL DEFAULT (datetime('now')))");
  const before = Date.now();
  db.prepare('INSERT INTO date_defaults(id) VALUES (?)').run(null);
  const date = db.prepare('SELECT * FROM date_defaults').get();
  assert.equal(date.id, 1);
  assert.ok(Math.abs(Date.parse(date.created + 'Z') - before) < 5000);
  for (const target of [db, oracle]) target.exec('CREATE TABLE timestamp_defaults(id INTEGER PRIMARY KEY, created TEXT DEFAULT CURRENT_TIMESTAMP)');
  assert.deepEqual(db.prepare('PRAGMA table_info(timestamp_defaults)').all(), oracle.prepare('PRAGMA table_info(timestamp_defaults)').all().map(row => ({ ...row })));
  for (const sql of ['INSERT INTO timestamp_defaults(id) VALUES (1)', 'INSERT INTO timestamp_defaults(id,created) VALUES (2,CURRENT_TIMESTAMP)', 'UPDATE timestamp_defaults SET created=CURRENT_TIMESTAMP WHERE id=1']) db.prepare(sql).run();
  const timestamps = db.prepare('SELECT created,CURRENT_TIMESTAMP AS now FROM timestamp_defaults').all();
  assert.equal(timestamps.length, 2);
  assert.ok(timestamps.every(row => Math.abs(Date.parse(row.created + 'Z') - Date.parse(row.now + 'Z')) < 5000));
  const generated="CREATE TABLE generated_defaults(id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(8)))),created TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),payload TEXT)";
  for(const target of [db,oracle])target.exec(generated);
  assert.deepEqual(db.prepare('PRAGMA table_info(generated_defaults)').all(),oracle.prepare('PRAGMA table_info(generated_defaults)').all().map(row=>({...row})));
  for(const target of [db,oracle])for(let n=0;n<8;n++)target.prepare('INSERT INTO generated_defaults(payload) VALUES (?)').run('synthetic-'+n);
  for(const target of [db,oracle]) {
    const rows=target.prepare('SELECT id,created FROM generated_defaults').all();assert.equal(new Set(rows.map(row=>row.id)).size,8);
    assert.ok(rows.every(row=>/^[a-f0-9]{16}$/.test(row.id)&&/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/.test(row.created)&&Math.abs(Date.parse(row.created)-Date.now())<5000));
    target.prepare('INSERT INTO generated_defaults(id,created,payload) VALUES (?,?,?)').run('explicit','2099-01-01T00:00:00Z','explicit fields');
  }
  assert.deepEqual(db.prepare('SELECT * FROM generated_defaults WHERE id=?').get('explicit'),{...oracle.prepare('SELECT * FROM generated_defaults WHERE id=?').get('explicit')});
  for(const target of [db,oracle]) {
    target.exec('CREATE TABLE datetime_affinity(id INTEGER PRIMARY KEY,value DATETIME DEFAULT CURRENT_TIMESTAMP)');
    for(const [id,value] of [[1,'2099-01-01T00:00:00Z'],[2,'ordinary-text'],[3,42],[4,4.25],[5,null]])target.prepare('INSERT INTO datetime_affinity(id,value) VALUES (?,?)').run(id,value);
    target.exec('CREATE TABLE range_checks(id INTEGER PRIMARY KEY,n INTEGER CHECK(n BETWEEN 1 AND 5))');
    for(const [id,n] of [[1,null],[2,1],[3,3.5],[4,5]])target.prepare('INSERT INTO range_checks(id,n) VALUES (?,?)').run(id,n);
    for(const n of [0,6])assert.throws(()=>target.prepare('INSERT INTO range_checks(n) VALUES (?)').run(n));
    target.exec("CREATE TABLE _omniroute_migrations(version TEXT PRIMARY KEY,name TEXT NOT NULL,applied_at TEXT NOT NULL DEFAULT (datetime('now')))");
    target.prepare('INSERT INTO _omniroute_migrations(version,name,applied_at) VALUES (?,?,?)').run('001','synthetic-upstream-history','2099-01-01 00:00:00');
  }
  assert.deepEqual(db.prepare('PRAGMA table_info(datetime_affinity)').all(),oracle.prepare('PRAGMA table_info(datetime_affinity)').all().map(row=>({...row})));
  for(const statement of ['SELECT * FROM datetime_affinity ORDER BY id','SELECT * FROM _omniroute_migrations ORDER BY version'])assert.deepEqual(db.prepare(statement).all(),oracle.prepare(statement).all().map(row=>({...row})));
  assert.deepEqual(db.prepare('PRAGMA table_info(_omniroute_migrations)').all(),oracle.prepare('PRAGMA table_info(_omniroute_migrations)').all().map(row=>({...row})));
  for(const target of [db,oracle]) {
    target.exec('CREATE TABLE datetime_unique(id INTEGER PRIMARY KEY,value DATETIME)');
    target.exec('CREATE UNIQUE INDEX datetime_partial_unique ON datetime_unique(value) WHERE value IS NOT NULL');
    for(const value of ['2099-01-01T00:00:00Z',42,Buffer.from([1,2]),null,null])target.prepare('INSERT INTO datetime_unique(value) VALUES (?)').run(value);
    for(const value of ['2099-01-01T00:00:00Z',42,Buffer.from([1,2])])assert.throws(()=>target.prepare('INSERT INTO datetime_unique(value) VALUES (?)').run(value));
    assert.equal(target.prepare('SELECT COUNT(*) AS n FROM datetime_unique').get().n,5);
  }
  for(const args of [[1,5],[null,3],[3,null],[null,null]]) {
    const statement='SELECT id,n BETWEEN ? AND ? AS inside,n NOT BETWEEN ? AND ? AS outside FROM range_checks ORDER BY id';
    assert.deepEqual(db.prepare(statement).all(...args,...args),oracle.prepare(statement).all(...args,...args).map(row=>({...row})));
  }
  for(const statement of ["CREATE TABLE rejected_random(id TEXT DEFAULT (lower(hex(randomblob(16)))))","CREATE TABLE rejected_clock(id TEXT DEFAULT (strftime('%s', 'now')))","ALTER TABLE generated_defaults ADD COLUMN extra TEXT DEFAULT (datetime('now'))",'SELECT * FROM _ha_serial']) {
    const rejected=new MongoAdapter({uri:test.uri,database:test.database});try{assert.throws(()=>rejected.prepare(statement).all(),{code:'HA_UNSUPPORTED_SQL'});}finally{await rejected.close();}
  }
  const numericDate=new MongoAdapter({uri:test.uri,database:test.database});
  try{assert.throws(()=>numericDate.prepare('INSERT INTO datetime_affinity(id,value) VALUES (?,?)').run(6,'42'),{code:'HA_UNSUPPORTED_SQL'});}finally{await numericDate.close();}
  const generatedSchema=await test.db.collection('_ha_schema').findOne({_id:'generated_defaults'});
  for(const change of [{'columns.0.defaultKind':'unknown-future-generator'},{'columns.0.type':'UNKNOWN_FUTURE_TYPE'}]) {
    await test.db.collection('_ha_schema').updateOne({_id:'generated_defaults'},{$set:change});
    const future=new MongoAdapter({uri:test.uri,database:test.database});
    try {
      future.exec('BEGIN IMMEDIATE');
      future.prepare('INSERT INTO key_value(namespace,key,value) VALUES (?,?,?)').run('settings','before-schema-fault','must-roll-back');
      assert.throws(()=>future.prepare('INSERT INTO generated_defaults(payload) VALUES (?)').run('must-not-write'),{code:'HA_SCHEMA_MISMATCH'});
      assert.equal(future.healthy(),false,'Catching a schema error must not preserve readiness');
      assert.equal(future.canWrite(),false);
      assert.throws(()=>future.exec('COMMIT'),{code:'HA_COMPATIBILITY_FAULT'});
      assert.throws(()=>future.prepare('INSERT INTO key_value(namespace,key,value) VALUES (?,?,?)').run('settings','after-schema-fault','must-not-write'),{code:'HA_COMPATIBILITY_FAULT'});
      assert.equal(await test.db.collection('key_value').countDocuments({key:'before-schema-fault'}),0,'Caught schema fault must roll back previous transaction writes');
    } finally {await future.close();}
    await test.db.collection('_ha_schema').replaceOne({_id:'generated_defaults'},generatedSchema);
  }
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM generated_defaults WHERE payload=?').get('must-not-write').n,0);
  console.log('PASS real migration schema primitives: preserved ledger/private boundary, per-row defaults, DATETIME partial UNIQUE storage classes, BETWEEN/NULL checks, affinity refusal and caught unknown-schema readiness/write fencing');
  for (const target of [db, oracle]) target.exec('CREATE TABLE conflict_rows(id TEXT PRIMARY KEY, value TEXT UNIQUE, n INTEGER NOT NULL)');
  const noConflict = 'INSERT INTO conflict_rows(id,value,n) VALUES (?,?,?) ON CONFLICT(id) DO NOTHING';
  for (const args of [['a','one',1], ['a','ignored',2], ['b','two',2]]) {
    assert.deepEqual(db.prepare(noConflict).run(...args), oracle.prepare(noConflict).run(...args));
  }
  assert.equal(db.prepare(noConflict + ' RETURNING id,n').get('a','ignored',3), undefined);
  assert.deepEqual(db.prepare(noConflict + ' RETURNING id,n').all('c','three',3), oracle.prepare(noConflict + ' RETURNING id,n').all('c','three',3).map(r => ({...r})));
  assert.throws(() => db.prepare(noConflict).run('d','one',4), { code: 'SQLITE_CONSTRAINT_UNIQUE' });
  assert.throws(() => db.prepare(noConflict).run('a','ignored',null), { code: 'SQLITE_CONSTRAINT_NOTNULL' });
  assert.deepEqual(db.prepare('SELECT * FROM conflict_rows ORDER BY id').all(), oracle.prepare('SELECT * FROM conflict_rows ORDER BY id').all().map(r => ({...r})));
  for (const sql of ['SELECT id FROM conflict_rows WHERE value = ? COLLATE NOCASE ORDER BY id', 'SELECT id FROM conflict_rows WHERE value COLLATE NOCASE <> ? ORDER BY id', 'SELECT id FROM conflict_rows WHERE value <> ? COLLATE NOCASE ORDER BY value COLLATE NOCASE DESC']) {
    for (const value of ['ONE','Two','İ',null]) assert.deepEqual(db.prepare(sql).all(value), oracle.prepare(sql).all(value).map(r => ({...r})));
  }
  for (const target of [db, oracle]) {
    target.exec('-- DDL collation must not be normalized as an expression\nCREATE TABLE case_names(name TEXT NOT NULL COLLATE NOCASE UNIQUE)');
    target.prepare('INSERT INTO case_names(name) VALUES (?)').run('Alpha');
    assert.throws(() => target.prepare('INSERT INTO case_names(name) VALUES (?)').run('ALPHA'));
    target.prepare('INSERT INTO case_names(name) VALUES (?)').run('nul\u0000A');
    assert.throws(() => target.prepare('INSERT INTO case_names(name) VALUES (?)').run('NUL\u0000B'));
    target.prepare('INSERT INTO case_names(name) VALUES (?)').run('NUL\u0000BB');
  }
  // Compare identities: Node's built-in SQLite string getter truncates at NUL,
  // whereas the SQL collation itself still compares the complete stored value.
  const nulCompare = 'SELECT rowid AS id FROM case_names WHERE name = ? COLLATE NOCASE ORDER BY rowid';
  assert.deepEqual(db.prepare(nulCompare).all('NUL\u0000z'), oracle.prepare(nulCompare).all('NUL\u0000z').map(r => ({...r})));
  for (const target of [db, oracle]) {
    target.exec('CREATE TABLE boolean_affinity(value BOOLEAN)');
    for (const value of [0,1,2,0.5,null]) target.prepare('INSERT INTO boolean_affinity(value) VALUES (?)').run(value);
  }
  assert.deepEqual(db.prepare('SELECT * FROM boolean_affinity ORDER BY value').all(), oracle.prepare('SELECT * FROM boolean_affinity ORDER BY value').all().map(r => ({...r})));
  for (const target of [db, oracle]) target.exec('CREATE TABLE no_rowid(a TEXT, b INTEGER, PRIMARY KEY(a,b)) WITHOUT ROWID');
  const withoutInsert = 'INSERT INTO no_rowid(a,b) VALUES (?,?)';
  assert.deepEqual(db.prepare(withoutInsert).run('a',1), oracle.prepare(withoutInsert).run('a',1));
  assert.throws(() => db.prepare(withoutInsert).run(null,2), { code: 'SQLITE_CONSTRAINT_NOTNULL' });
  assert.deepEqual(db.prepare('PRAGMA table_info(no_rowid)').all(), oracle.prepare('PRAGMA table_info(no_rowid)').all().map(r => ({...r})));
  for (const target of [db, oracle]) {
    for (const file of ['155_agentic_conversations', '156_conversation_turn_nodes']) target.exec(readFileSync('upstream/omniroute/src/lib/db/migrations/' + file + '.sql', 'utf8'));
    for (const [id, fp, seen] of [['a','fp','2026-01-03'],['b','fp','2026-01-02'],['c','other','2026-01-04'],['d','fp','2026-01-01']]) {
      target.prepare("INSERT INTO agentic_conversations(id,api_key_id,fingerprint_hash,last_message_count,last_messages_hash,first_seen_at,last_seen_at) VALUES (?, 'key', ?, 0, '', ?, ?)").run(id,fp,seen,seen);
    }
    for (const [id,conversation,hash] of [['a1','a','x'],['a2','a','x'],['b1','b','x'],['b2','b','y'],['c1','c','x'],['c2','c','y'],['d1','d','x'],['d2','d','y']]) {
      target.prepare("INSERT INTO conversation_turn_nodes(id,conversation_id,parent_id,role,content_hash,first_seen_at,last_seen_at) VALUES (?, ?, NULL, 'user', ?, '2026-01-01', '2026-01-01')").run(id,conversation,hash);
    }
  }
  for (const args of [['x','y','fp',2,20], ['x','y','fp',0,20], ['x','y','other',500,1], ['absent','fp',500,20]]) {
    const sql = conversationCandidatesSql(args.length - 3);
    const expected = oracle.prepare(sql).all(...args).map(r => ({...r}));
    const revision = (await test.db.collection('_ha_serial').findOne({ _id: 'writer' })).revision;
    assert.deepEqual(db.prepare(sql).all(...args), expected);
    assert.equal((await test.db.collection('_ha_serial').findOne({ _id: 'writer' })).revision, revision, 'Conversation SELECT must not acquire a write fence');
    assert.deepEqual(db.transaction(() => db.prepare(sql).all(...args))(), expected);
  }
  const turnOrder = 'SELECT rowid AS seq, id FROM conversation_turn_nodes ORDER BY rowid DESC LIMIT 3';
  assert.deepEqual(db.prepare(turnOrder).all(), oracle.prepare(turnOrder).all().map(r => ({...r})));
  for (const target of [db, oracle]) {
    target.exec("CREATE TABLE not_checks(id TEXT PRIMARY KEY, n INTEGER, CHECK(NOT(n=1 AND id='bad')))");
    target.prepare('INSERT INTO not_checks(id,n) VALUES (?,?)').run('ok',1);
    target.prepare('INSERT INTO not_checks(id,n) VALUES (?,?)').run('nullable',null);
    assert.throws(() => target.prepare('INSERT INTO not_checks(id,n) VALUES (?,?)').run('bad',1));
    target.prepare('INSERT INTO not_checks(id,n) VALUES (?,?)').run('bad',null);
  }
  for (const target of [db, oracle]) {
    target.exec(readFileSync('upstream/omniroute/src/lib/db/migrations/013_quota_snapshots.sql','utf8'));
    for (const [connection,window,remaining,time] of [['one','hour',10,'2026-01-03'],['one','hour',20,'2026-01-03'],['one','week',30,'2026-01-01'],['two','hour',99,'2026-01-04']]) {
      target.prepare("INSERT INTO quota_snapshots(provider,connection_id,window_key,remaining_percentage,created_at) VALUES ('p',?,?,?,?)").run(connection,window,remaining,time);
    }
  }
  const byWindow = rows => rows.map(r=>({...r})).sort((a,b)=>a.window_key.localeCompare(b.window_key));
  const quotaRevision = (await test.db.collection('_ha_serial').findOne({ _id:'writer' })).revision;
  assert.deepEqual(byWindow(db.prepare(latestQuotaSql()).all('one')), byWindow(oracle.prepare(latestQuotaSql()).all('one')));
  assert.equal((await test.db.collection('_ha_serial').findOne({ _id:'writer' })).revision, quotaRevision);
  for (const target of [db, oracle]) {
    target.exec('CREATE TABLE text_metrics (id INTEGER PRIMARY KEY, content TEXT)');
    for (const [id, content] of [[1, 'abcde'], [2, 'سلام🙂'], [3, 'ab\u0000ignored'], [4, null]]) {
      target.prepare('INSERT INTO text_metrics(id,content) VALUES (?,?)').run(id, content);
    }
  }
  for (const sql of [
    'SELECT id, LENGTH(content) AS length FROM text_metrics ORDER BY id',
    'SELECT COALESCE(SUM((LENGTH(content) + 3) / 4), 0) AS tokensUsed FROM text_metrics',
    'SELECT SUM((LENGTH(content) + 3) / 4) AS tokensUsed FROM text_metrics WHERE id=4',
    'SELECT COALESCE(SUM((LENGTH(content) + 3) / 4), 0) AS tokensUsed FROM text_metrics WHERE id=99',
    'SELECT (3-8)/2 AS quotient, 1/0 AS zero',
    'SELECT 1.25 AS fractional, 1.25+2 AS addition',
  ]) assert.deepEqual(db.prepare(sql).all(), oracle.prepare(sql).all().map(row => ({ ...row })), sql);
  for (const target of [db, oracle]) {
    if (target === oracle) target.exec('PRAGMA foreign_keys=ON');
    target.exec('CREATE TABLE parents (id TEXT PRIMARY KEY)');
    target.exec('CREATE TABLE children (id TEXT PRIMARY KEY, parent_id TEXT REFERENCES parents(id) ON DELETE CASCADE, n INTEGER CHECK(n>0))');
    target.exec('CREATE TABLE join_names(id TEXT PRIMARY KEY,label TEXT)');
    for(const [index,name] of ['b','A','a\u0000x','a\u0000xx','ÖZ',null].entries()) target.prepare('INSERT INTO join_names(id,label) VALUES (?,?)').run(String(index),name);
    target.prepare('INSERT INTO parents(id) VALUES (?)').run('p');
    target.prepare('INSERT INTO children(id,parent_id,n) VALUES (?,?,?)').run('c', 'p', 2);
  }
  for(const direction of ['ASC','DESC']) {
    // Compare ordering by stable IDs: node:sqlite truncates a returned TEXT at
    // NUL, while its SQL collation still compares the stored byte length.
    const sortedJoin=`SELECT p.id FROM parents p JOIN join_names n ON p.id=n.id ORDER BY n.label COLLATE NOCASE ${direction}`;
    for(const target of [db,oracle]) for(let n=0;n<6;n++) target.prepare('INSERT OR IGNORE INTO parents(id) VALUES (?)').run(String(n));
    assert.deepEqual(db.prepare(sortedJoin).all(),oracle.prepare(sortedJoin).all().map(row=>({...row})));
  }
  for (const sql of [
    'SELECT p.* FROM children c JOIN parents p ON p.id=c.parent_id WHERE c.n > 0',
    'SELECT c.id AS child_id, p.id AS parent_id FROM children c INNER JOIN parents p ON p.id=c.parent_id ORDER BY c.n DESC',
    'SELECT 1 FROM children c JOIN parents p ON p.id=c.parent_id WHERE c.n > 0 LIMIT 1',
    'SELECT c.id FROM children c LEFT JOIN parents p ON p.id=c.parent_id ORDER BY c.id LIMIT 2 OFFSET 1',
    'SELECT c.id FROM children c LEFT JOIN parents p ON p.id=c.parent_id ORDER BY c.id LIMIT 0 OFFSET 1',
    'SELECT c.id FROM children c LEFT JOIN parents p ON p.id=c.parent_id ORDER BY c.id LIMIT 2 OFFSET -1',
    "SELECT c.id,CAST(c.n AS TEXT) AS status,CAST(p.id AS TEXT) AS label FROM children c LEFT JOIN parents p ON p.id=c.parent_id WHERE CAST(c.n AS TEXT) LIKE '%2%' ORDER BY c.id LIMIT 2 OFFSET 0",
    "SELECT c.*, p.id AS parent, x.id AS missing, COALESCE(NULLIF(p.id,''),c.id) AS label FROM children c LEFT JOIN parents p ON p.id=c.parent_id LEFT JOIN parents x ON x.id=c.id ORDER BY c.id",
  ]) assert.deepEqual(db.prepare(sql).all(), oracle.prepare(sql).all().map(row => ({ ...row })));
  for(const target of [db,oracle]){
    target.exec('CREATE TABLE joined_cast_values(id TEXT PRIMARY KEY,n INTEGER)');
    for(const [index,value]of [null,0,-1,2147483648,100000000000000,Number.MAX_SAFE_INTEGER].entries())
      target.prepare('INSERT INTO joined_cast_values(id,n) VALUES (?,?)').run(String(index),value);
  }
  const castSql='SELECT v.id,CAST(v.n AS TEXT) AS value FROM joined_cast_values v LEFT JOIN parents p ON p.id=v.id ORDER BY v.id';
  assert.deepEqual(db.prepare(castSql).all(),oracle.prepare(castSql).all().map(row=>({...row})));
  const searchSql='SELECT c.id FROM children c LEFT JOIN parents p ON p.id=c.parent_id LEFT JOIN parents x ON x.id=c.id WHERE ('+
    [...Array.from({length:14},(_,index)=>(index%2?'c.id':'x.id')+' LIKE @query'),'CAST(c.n AS TEXT) LIKE @query'].join(' OR ')+') ORDER BY c.id LIMIT 2 OFFSET 0';
  for(const query of ['%2%','%missing%'])assert.deepEqual(db.prepare(searchSql).all({query}),oracle.prepare(searchSql).all({query}).map(row=>({...row})));
  console.log('PASS wide call-log search predicate vs SQLite: 15 OR terms, NULL three-valued logic, status cast and pagination execute within MongoDB BSON limits');
  for(const value of [0.25,1e16]){
    db.prepare('INSERT INTO joined_cast_values(id,n) VALUES (?,?)').run('uncertified',value);
    const castReader=new MongoAdapter({uri:test.uri,database:test.database});
    try{assert.throws(()=>castReader.prepare(castSql).all(),{code:'HA_UNSUPPORTED_SQL'});assert.ok(castReader.compatibilityFault);}
    finally{castReader.close();db.prepare('DELETE FROM joined_cast_values WHERE id=?').run('uncertified');}
  }
  console.log('PASS joined status TEXT casts vs SQLite: NULL/signed/large safe integers and TEXT affinity; floating/unsafe-integer data refuse explicitly without truncation');
  for (const target of [db, oracle]) {
    target.exec('CREATE TABLE nullable_uniques (id TEXT PRIMARY KEY, value TEXT)');
    target.exec('CREATE UNIQUE INDEX unique_nonnull ON nullable_uniques(value)');
    target.prepare('INSERT INTO nullable_uniques(id,value) VALUES (?,?)').run('a',null);
    target.prepare('INSERT INTO nullable_uniques(id,value) VALUES (?,?)').run('b',null);
  }
  db.prepare('INSERT INTO nullable_uniques(id,value) VALUES (?,?)').run('c','v');
  assert.throws(() => db.prepare('INSERT INTO nullable_uniques(id,value) VALUES (?,?)').run('d','v'), { code: 'SQLITE_CONSTRAINT_UNIQUE' });
  oracle.prepare('INSERT INTO nullable_uniques(id,value) VALUES (?,?)').run('c','v');
  for(const target of [db,oracle]) {
    const sql='INSERT INTO nullable_uniques(id,value) VALUES (?,?) ON CONFLICT(value) DO UPDATE SET id=excluded.id';
    target.prepare(sql).run('updated-c','v');target.prepare(sql).run('another-null',null);
  }
  assert.deepEqual(db.prepare('SELECT * FROM nullable_uniques ORDER BY id').all(),oracle.prepare('SELECT * FROM nullable_uniques ORDER BY id').all().map(row=>({...row})));
  console.log('PASS standalone UNIQUE index targets: SQLite-differential UPSERT and multiple NULL keys');
  db.exec('CREATE TABLE bounded (id TEXT PRIMARY KEY, value INTEGER)');
  for (let n=0;n<4;n++) db.prepare('INSERT INTO bounded(id,value) VALUES (?,?)').run(String(n),n);
  db.prepare('DELETE FROM bounded WHERE rowid IN (SELECT rowid FROM bounded WHERE value < ? LIMIT ?)').run(3,2);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM bounded').get().n,2);
  assert.throws(() => db.prepare('INSERT INTO children(id,parent_id,n) VALUES (?,?,?)').run('bad', 'missing', 1), { code: 'SQLITE_CONSTRAINT_FOREIGNKEY' });
  assert.throws(() => db.prepare('INSERT INTO children(id,parent_id,n) VALUES (?,?,?)').run('bad', 'p', 0), { code: 'SQLITE_CONSTRAINT_CHECK' });
  assert.throws(() => db.transaction(() => { db.prepare('DELETE FROM parents WHERE id=?').run('p'); throw new Error('rollback cascade'); })(), /rollback cascade/);
  assert.equal(db.prepare('SELECT COUNT(*) AS n FROM children').get().n, 1);
  for (const target of [db, oracle]) target.prepare('DELETE FROM parents WHERE id=?').run('p');
  assert.deepEqual(db.prepare('SELECT * FROM children').all(), oracle.prepare('SELECT * FROM children').all());
  for (const target of [db, oracle]) target.exec('CREATE TABLE returning_rows(id TEXT PRIMARY KEY, n INTEGER NOT NULL)');
  for (const [sql,args] of [
    ['INSERT INTO returning_rows(id,n) VALUES (?,?) RETURNING id,n', ['a',1]],
    ['INSERT INTO returning_rows(id,n) VALUES (?,?) ON CONFLICT(id) DO UPDATE SET n=n+excluded.n RETURNING n', ['a',2]],
    ['UPDATE returning_rows SET n=n+1 WHERE id=? RETURNING *', ['a']],
    ['DELETE FROM returning_rows WHERE id=? RETURNING id', ['a']],
  ]) assert.deepEqual(db.prepare(sql).all(...args), oracle.prepare(sql).all(...args).map(row => ({...row})), sql);
  // An upstream catch must not hide an unsupported statement and then commit
  // earlier work or continue writing through an already prepared statement.
  const prepared = db.prepare(insert);
  assert.throws(() => db.transaction(() => {
    prepared.run('before-unsupported', 'must roll back');
    assert.throws(() => db.prepare('DELETE FROM key_value WHERE unknown_function(key)').run(), { code: 'HA_UNSUPPORTED_SQL' });
    assert.throws(() => prepared.run('after-unsupported', 'must not run'), { code: 'HA_COMPATIBILITY_FAULT' });
  })(), { code: 'HA_COMPATIBILITY_FAULT' });
  assert.equal(peer.prepare("SELECT key FROM key_value WHERE key IN ('before-unsupported', 'after-unsupported')").all().length, 0);
  assert.equal(db.healthy(), false);
  assert.throws(() => db.transaction(() => {})(), { code: 'HA_COMPATIBILITY_FAULT' });
  peer.exec('BEGIN IMMEDIATE');
  peer.prepare(insert).run('before-parse-error', 'must roll back');
  assert.throws(() => peer.prepare("SELECT 'unterminated"), { code: 'HA_UNSUPPORTED_SQL' });
  assert.throws(() => peer.exec('COMMIT'), { code: 'HA_COMPATIBILITY_FAULT' });
  assert.equal(await test.db.collection('key_value').countDocuments({ key: 'before-parse-error' }), 0);
  // The rejected COMMIT must release the writer row, not leave an abandoned
  // transaction blocking promotion until MongoDB's transaction timeout.
  await test.db.collection('_ha_serial').updateOne({ _id: 'writer' }, { $inc: { revision: 1 } }, { maxTimeMS: 1000 });
  assert.equal(peer.healthy(), false);
  console.log('PASS MongoDB adapter: SQLite differential queries, bindings, constraints, rollback, visibility, writer conflict, BLOB');
  console.log('PASS schema installation: rejected metadata rolls back collection creation; retry installs the complete schema');
  console.log('PASS fail-closed compatibility: caught unsupported SQL cannot commit partial work or execute subsequent prepared writes');
} finally {
  await db?.close(); await peer?.close(); oracle.close(); await test.cleanup();
}
