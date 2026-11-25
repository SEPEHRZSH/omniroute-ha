import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const cwd = fileURLToPath(new URL('../', import.meta.url));
const env = { ...process.env, NEXT_TELEMETRY_DISABLED: '1', NEXT_PRIVATE_OUTPUT_TRACE_ROOT: cwd, GIT_OPTIONAL_LOCKS: '0' };
if (process.argv.includes('--full-backend')) env.HA_TEST_FULL_BACKEND = '1';
if (process.argv.includes('--http-only') || process.argv.includes('--mongo-election')) env.HA_TEST_FULL_BACKEND = '1';
if (process.argv.includes('--mongo-election')) env.HA_TEST_MONGO_ELECTION = '1';
if (process.argv.includes('--data-recovery')) {
  env.HA_TEST_DATA_RECOVERY = '1'; env.HA_TEST_FULL_BACKEND = '1';
}
if (process.argv.includes('--reservations')) { env.HA_TEST_RESERVATIONS = '1'; env.HA_TEST_FULL_BACKEND = '1'; }
async function run(command, args) {
  const result = await new Promise((resolve, reject) => {
    const child = spawn(command, args, { cwd, env, stdio: ['ignore', 'pipe', 'pipe'] });
    let output = ''; const evidence=[];
    for(const stream of [child.stdout,child.stderr]) {
      let pending='';
      const record=line=>{if(/^(PASS |UPSTREAM_OK|RESOURCE )/.test(line)) {evidence.push(line.slice(0,2000));if(evidence.length>512)evidence.shift();}};
      stream.on('data',chunk=>{
        const text=chunk.toString();output=(output+text).slice(-32000);
        const lines=(pending+text).split('\n');pending=lines.pop().slice(-64000);
        for(const line of lines) record(line);
      });
      stream.on('end',()=>record(pending));
    }
    child.on('error', reject);
    child.on('close', code => resolve({ code, output,evidence }));
  });
  if (result.code !== 0) { process.stderr.write(result.output); throw new Error(`Test failed: ${args.join(' ')}`); }
  for (const line of result.evidence) console.log(line);
}
try {
  await run('bash', ['scripts/verify-upstream.sh']);
  if (process.argv.includes('--http-only')) {
    const { verifyBuildContract } = await import('../adapter/buildContract.mjs');
    verifyBuildContract(fileURLToPath(new URL('../.work/backend', import.meta.url)));
    await run(process.execPath, ['--import', './adapter/register.mjs', 'tests/httpFailover.mjs']);
  } else {
  await run(process.execPath, ['tests/baseSchema.mjs']);
  await run(process.execPath, ['--import', './adapter/register.mjs', 'tests/startupBoundary.mjs']);
  for (const file of ['overlay', 'redisConfig', 'syncBridge', 'mongoAdapter', 'ownership', 'databaseFailures', 'providerRetirement','freeProxies', 'dataLifecycle', 'reservations', 'poolQuota','gamification','credentialImports','kiroRefresh','usageRollup']) await run(process.execPath, [`tests/${file}.mjs`]);
  for (const file of ['standbyStartup', 'activeStartup', 'omnirouteDomains', 'memory', 'quota']) await run(process.execPath, ['--import', './adapter/register.mjs', `tests/${file}.mjs`]);
  if (!process.argv.includes('--reuse-build')) await run(process.execPath, ['node_modules/next/dist/bin/next', 'build', 'tests/nextApp', '--webpack']);
  await run(process.execPath, ['tests/compiledOverlay.mjs']);
  await run(process.execPath, ['tests/httpBoundary.mjs']);
  await run(process.execPath, ['--import', './adapter/register.mjs', 'tests/httpFailover.mjs']);
  }
} finally { await run('bash', ['scripts/verify-upstream.sh']); }
