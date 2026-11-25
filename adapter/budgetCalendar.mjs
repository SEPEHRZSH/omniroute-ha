// Reuse only the pinned upstream pure calendar functions. No application module
// initialization, DB globals or network code executes in the synchronous worker.
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { stripTypeScriptTypes } from 'node:module';
const source = readFileSync(new URL('../upstream/omniroute/src/domain/costRules.ts', import.meta.url), 'utf8');
if (createHash('sha256').update(source).digest('hex') !== '804bdc9d42022d2069dd0ec27ecc22c62aff03dfd5d7617275fbce05f63603e3') throw new Error('Budget calendar source changed');
function section(start, end) {
  if (source.split(start).length !== 2 || source.split(end).length !== 2) throw new Error('Budget calendar extraction anchor changed');
  return source.slice(source.indexOf(start), source.indexOf(end));
}
// Keep the exact normalization and UTC daily/weekly/monthly boundary behavior.
const regex = source.match(/^const RESET_TIME_REGEX = [^\n]+$/m)?.[0];
if (!regex) throw new Error('Budget reset expression missing');
export const getBudgetWindow = new Function(stripTypeScriptTypes(regex + '\n' +
  section('function normalizeResetTime(', 'function normalizeTimestamp(') + '\n' +
  section('function getResetTimeParts(', 'function getActiveBudgetLimit(').replace('export function', 'function')) +
  '\nreturn getBudgetWindow;')();
