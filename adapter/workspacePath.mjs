// Reject symlinks before accessing selected paths to keep operations within
// the workspace and outside the read-only upstream checkout.
import { lstatSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
const root = path.resolve(fileURLToPath(new URL('../',import.meta.url)));
export function workspacePath(value,{directory=false,existing=true}={}) {
  if (typeof value !== 'string' || !value.trim()) throw new Error('Explicit workspace path required');
  const absolute = path.resolve(value), upstream = path.join(root,'upstream');
  if (!absolute.startsWith(root+path.sep) || absolute===upstream || absolute.startsWith(upstream+path.sep)) throw new Error('Path must stay inside workspace, outside upstream');
  let current = root;
  const segments = path.relative(root,absolute).split(path.sep);
  for (const [index,segment] of segments.entries()) {
    current = path.join(current,segment);
    const last = index===segments.length-1;
    let stat;
    try { stat = lstatSync(current); }
    catch(error) { if (!existing && last && error.code==='ENOENT') return absolute; throw error; }
    if (stat.isSymbolicLink()) throw new Error('Symlink workspace paths are not allowed');
    if (last ? directory ? !stat.isDirectory() : !stat.isFile() : !stat.isDirectory()) throw new Error('Unexpected workspace path type');
  }
  return absolute;
}
