// Start the compiled application after verifying the HA build contract.
import './register.mjs';
import { createRequire } from 'node:module';
import { createServer } from 'node:http';
import { fileURLToPath } from 'node:url';
import { randomUUID } from 'node:crypto';
import path from 'node:path';
import { readFileSync } from 'node:fs';
import { verifyBuildContract } from './buildContract.mjs';
import { MongoAdapter } from './mongoAdapter.mjs';
import { attachActiveRuntime } from './activeRuntime.mjs';
import { fencedHttpHandler } from './http.mjs';
import { workspacePath } from './workspacePath.mjs';
import {nativeRevision} from './runtimeIdentity.mjs';

const root = path.resolve(fileURLToPath(new URL('../', import.meta.url)));
process.env.TZ ||= 'UTC'; // Set before MongoDB's worker inherits the environment.
const appDirectory = workspacePath(process.env.HA_NEXT_DIR || path.join(root,'.work/backend'),{directory:true});
verifyBuildContract(appDirectory);
const completion = JSON.parse(readFileSync(path.join(appDirectory,'.next/ha-complete.json'),'utf8'));
const pages = JSON.parse(readFileSync(path.join(appDirectory,'.next/server/app-paths-manifest.json'),'utf8'));
if (completion.dashboard !== true || !(completion.uiEntries > 0) ||
    ['/login/page','/docs/page','/(dashboard)/dashboard/page'].some(page => !pages[page])) {
  throw new Error('Deployment requires the complete dashboard/docs build; backend-only artifacts are diagnostic');
}
for (const name of ['HA_MONGO_URI', 'HA_MONGO_DATABASE', 'DATA_DIR', 'JWT_SECRET', 'API_KEY_SECRET', 'STORAGE_ENCRYPTION_KEY', 'OMNIROUTE_HA_IDENTITY']) {
  if (!process.env[name]?.trim()) throw new Error('Required HA environment variable: ' + name);
}
for (const name of ['OMNIROUTE_HEADLESS', 'OMNIROUTE_DISABLE_BACKGROUND_SERVICES', 'OMNIROUTE_DISABLE_CREDENTIAL_HEALTH_CHECK', 'OMNIROUTE_DISABLE_REDIS_AUTH_CACHE', 'DISABLE_SQLITE_AUTO_BACKUP']) {
  if (!['1', 'true'].includes(process.env[name])) throw new Error('Validated HA profile requires ' + name + '=true');
}
workspacePath(process.env.DATA_DIR,{directory:true});
const port = Number(process.env.PORT || 20128);
if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error('Invalid PORT');
process.env.NEXT_PRIVATE_OUTPUT_TRACE_ROOT = root;
if(process.env.HA_STATUS_TOKEN&&process.env.HA_STATUS_TOKEN.length<32)throw new Error('HA_STATUS_TOKEN requires at least 32 characters');
const instanceId=randomUUID();
const db = new MongoAdapter({ uri: process.env.HA_MONGO_URI, database: process.env.HA_MONGO_DATABASE, ownerId: instanceId });
const runtime = attachActiveRuntime(db);
const runtimeRevision=nativeRevision();
const next = createRequire(import.meta.url)('next');
const app = next({ dev: false, dir: appDirectory });
let prepared = false;
const server = createServer(fencedHttpHandler({
  ready: () => prepared && runtime.ready(),
  status:()=>{const active=prepared&&runtime.ready();return{active,role:active?'active':runtime.retired?'retired':runtime.owner.state==='promoting'?'promoting':prepared?'standby':'starting',instanceId,epoch:runtime.owner.epoch??null,buildId:completion.buildId,nativeRevision:runtimeRevision,sharedFingerprint:process.env.HA_SHARED_FINGERPRINT??null};},
}, app.getRequestHandler(),{statusToken:process.env.HA_STATUS_TOKEN}));
// Upgrade protocols need their own validated ownership lifetime; fail closed.
server.on('upgrade', (_request, socket) => socket.destroy());
await new Promise((resolve, reject) => {
  server.once('error', reject);
  server.listen(port, process.env.HOST || '127.0.0.1', resolve);
});
// Bind first so even an ephemeral test port is valid before instrumentation
// validates the web environment. The listener stays fail-closed during prepare.
process.env.API_PORT = String(server.address().port);
process.env.DASHBOARD_PORT = process.env.API_PORT;
try {
  await app.prepare(); // Actual app instrumentation registers the fenced boot callback.
  if (!runtime.boot) throw new Error('Built app did not register HA instrumentation');
  prepared = true;
} catch (error) {
  server.closeAllConnections(); server.close(); await db.close(); throw error;
}
console.log('[HA] HTTP listener started; /readyz reflects preparation and active ownership');
process.send?.({ port: server.address().port }); // Optional supervisor notification, not a readiness claim.
