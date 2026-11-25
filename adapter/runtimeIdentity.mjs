// Path-independent identity of the native runtime accompanying a Next artifact.
// The compiled build contract alone does not cover these external modules.
import {createHash} from 'node:crypto';
import {readFileSync,readdirSync} from 'node:fs';
export function nativeRevision(){
  const directory=new URL('./',import.meta.url),hash=createHash('sha256');
  for(const name of readdirSync(directory).filter(name=>/\.(?:mjs|cjs)$/.test(name)).sort())hash.update(name+'\0').update(readFileSync(new URL(name,directory)));
  return hash.digest('hex');
}
