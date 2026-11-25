import { registerHooks, createRequire } from 'node:module';
import { readFileSync, existsSync } from 'node:fs';
import { fileURLToPath, pathToFileURL } from 'node:url';
import path from 'node:path';
import { transformSync } from 'esbuild';
import { overlay } from './overlay.mjs';

const workspace = fileURLToPath(new URL('../', import.meta.url));
const upstream = path.join(workspace, 'upstream/omniroute');
const projectRequire = createRequire(new URL('../package.json', import.meta.url));
function candidate(base) {
  for (const name of [base, `${base}.ts`, `${base}.tsx`, `${base}.js`, `${base}/index.ts`, `${base}/index.js`]) {
    if (/\.(?:[cm]?js|tsx?|json)$/.test(name) && existsSync(name)) return pathToFileURL(name).href;
  }
}

registerHooks({
  resolve(specifier, context, nextResolve) {
    if (['node:sqlite', 'better-sqlite3', 'sql.js', 'bun:sqlite', 'sqlite-vec'].includes(specifier)) {
      throw new Error(`SQLite dependency forbidden in MongoDB application runner: ${specifier}`);
    }
    let base;
    if (specifier.startsWith('@/')) base = path.join(upstream, 'src', specifier.slice(2));
    else if (specifier.startsWith('@omniroute/open-sse/')) base = path.join(upstream, 'open-sse', specifier.slice('@omniroute/open-sse/'.length));
    else if (specifier.startsWith('.') && context.parentURL?.startsWith(pathToFileURL(upstream + '/').href)) {
      base = path.resolve(path.dirname(fileURLToPath(context.parentURL)), specifier);
    }
    if (base) {
      const url = candidate(base) || (/\.js$/.test(base) && candidate(base.slice(0, -3)));
      if (url) return { url, shortCircuit: true };
    }
    try { return nextResolve(specifier, context); }
    catch (error) {
      if (!specifier.startsWith('.') && !specifier.includes(':')) return { url: pathToFileURL(projectRequire.resolve(specifier)).href, shortCircuit: true };
      throw error;
    }
  },
  load(url, context, nextLoad) {
    if (url.startsWith(pathToFileURL(upstream + '/').href) && url.endsWith('.json')) {
      const data = JSON.parse(readFileSync(fileURLToPath(url), 'utf8'));
      const named = fileURLToPath(url) === path.join(upstream, 'package.json') ? 'export const version = data.version;' : '';
      return { source: `const data = ${JSON.stringify(data)}; export default data; ${named}`, format: 'module', shortCircuit: true };
    }
    if (url.startsWith(pathToFileURL(upstream + '/').href) && /\.[jt]sx?$/.test(url)) {
      const filename = fileURLToPath(url);
      const source = overlay(path.relative(upstream, filename), readFileSync(filename, 'utf8'));
      return { source: transformSync(source, { loader: filename.endsWith('tsx') ? 'tsx' : filename.endsWith('.ts') ? 'ts' : 'js', format: 'esm', target: 'node22', sourcefile: filename }).code, format: 'module', shortCircuit: true };
    }
    return nextLoad(url, context);
  },
});
