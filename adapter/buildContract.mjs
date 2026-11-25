import { readFileSync } from 'node:fs';
import { isDeepStrictEqual } from 'node:util';
import path from 'node:path';
import { overlayPins } from './overlay.mjs';
import { createHash } from 'node:crypto';

export function overlayFingerprint() {
  const hash = createHash('sha256');
  for (const file of ['overlay.mjs', 'webpackOverlay.cjs', 'register.mjs', 'buildGuard.cjs','intlCompiler.cjs', 'buildContract.mjs', '../scripts/build-backend.mjs', '../package-lock.json']) hash.update(readFileSync(new URL(file, import.meta.url)));
  return hash.digest('hex');
}

export function verifyBuildContract(appDirectory) {
  const raw = readFileSync(path.join(appDirectory, '.next/server/chunks/omniroute-ha-build.json'));
  const contract = JSON.parse(raw.toString('utf8'));
  if (contract.upstream !== '191dce8d534c' || contract.overlay !== overlayFingerprint() || !isDeepStrictEqual(contract.pins, overlayPins)) throw new Error('HA build is for a different source/overlay revision');
  for (const filename of ['src/lib/db/core.ts', 'src/lib/db/adapters/runtimeRequire.ts', 'src/instrumentation-node.ts', ...Object.keys(overlayPins)]) {
    if (!contract.applied?.includes(filename)) throw new Error('HA build is missing a required integration: ' + filename);
  }
  let completed,buildId;
  try { completed=JSON.parse(readFileSync(path.join(appDirectory,'.next/ha-complete.json'),'utf8')); buildId=readFileSync(path.join(appDirectory,'.next/BUILD_ID'),'utf8').trim(); }
  catch { throw new Error('HA backend build has no valid completion record'); }
  if (!buildId || completed.buildId!==buildId || completed.contractSha256!==createHash('sha256').update(raw).digest('hex') ||
      completed.kind!=='omniroute-ha-backend' || !Number.isSafeInteger(completed.routeHandlers) || completed.routeHandlers<1) throw new Error('HA backend build completion does not match its artifact');
  return contract;
}
