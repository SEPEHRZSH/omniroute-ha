import assert from 'node:assert/strict';
import { readFileSync,writeFileSync,mkdirSync,mkdtempSync,rmSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { overlayPins } from '../adapter/overlay.mjs';
import path from 'node:path';
import { overlayFingerprint, verifyBuildContract } from '../adapter/buildContract.mjs';
import { createRequire } from 'node:module';

// No Node source loader or MongoDB here: exercise the server bundle Next built.
const load = createRequire(import.meta.url);
load('next/dist/server/node-environment');
const { routeModule } = load('./nextApp/.next/server/app/api/[...path]/route.js');
const response = await routeModule.userland.GET(new Request('http://localhost/api/settings'));
assert.equal(response.status, 503);
assert.deepEqual(await response.json(), { error: 'Test runtime unavailable' });
console.log('PASS compiled Next.js server bundle enforces the MongoDB driver boundary without a runtime source-loader hook');

const app = path.resolve('tests/nextApp');
const contract = JSON.parse(readFileSync(path.join(app,'.next/server/chunks/omniroute-ha-build.json'),'utf8'));
assert.equal(contract.overlay, overlayFingerprint());
assert.ok(contract.applied.includes('src/lib/db/adapters/runtimeRequire.ts'));
assert.throws(() => verifyBuildContract(app), /missing a required integration/);
console.log('PASS build contract: current overlay digest recorded; deployment entry rejects the focused test build as incomplete');

const fixture=mkdtempSync(path.resolve('.work/build-contract-'));
try {
  mkdirSync(path.join(fixture,'.next/server/chunks'),{recursive:true});
  const raw=JSON.stringify({upstream:'191dce8d534c',overlay:overlayFingerprint(),pins:overlayPins,applied:Object.keys(overlayPins)});
  writeFileSync(path.join(fixture,'.next/server/chunks/omniroute-ha-build.json'),raw);
  writeFileSync(path.join(fixture,'.next/BUILD_ID'),'synthetic-build');
  assert.throws(()=>verifyBuildContract(fixture),/completion record/);
  const completed={kind:'omniroute-ha-backend',routeHandlers:797,buildId:'synthetic-build',contractSha256:createHash('sha256').update(raw).digest('hex')};
  writeFileSync(path.join(fixture,'.next/ha-complete.json'),JSON.stringify({...completed,buildId:'stale-build'}));
  assert.throws(()=>verifyBuildContract(fixture),/completion does not match/);
  writeFileSync(path.join(fixture,'.next/ha-complete.json'),JSON.stringify(completed));
  assert.equal(verifyBuildContract(fixture).upstream,'191dce8d534c');
  writeFileSync(path.join(fixture,'.next/server/chunks/omniroute-ha-build.json'),raw+'\n');
  assert.throws(()=>verifyBuildContract(fixture),/completion does not match/);
} finally {rmSync(fixture,{recursive:true});}
console.log('PASS completed-build gate: valid compiler pins alone cannot authorize an unfinished build; missing/stale completion and mixed artifact identity reject startup (synthetic metadata fixture)');
