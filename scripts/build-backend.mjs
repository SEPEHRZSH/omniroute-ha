// Build the official app without copying or modifying upstream sources.
// --backend-only retains the smaller diagnostic profile.
import { readFileSync, writeFileSync, mkdirSync, readdirSync, symlinkSync, existsSync, lstatSync, readlinkSync, unlinkSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import {workspacePath} from '../adapter/workspacePath.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
const upstream = path.join(root, 'upstream/omniroute');
const app = path.join(root, '.work/backend');
if(process.argv.slice(2).some(arg=>arg!=='--backend-only'))throw new Error('Unknown build option');
const dashboard=!process.argv.includes('--backend-only');
function verify() {
  const result = spawnSync('bash', ['scripts/verify-upstream.sh'], { cwd: root, env: { ...process.env, GIT_OPTIONAL_LOCKS: '0' }, stdio: 'inherit' });
  if (result.error || result.signal || result.status !== 0) throw new Error('Upstream verification failed: '+(result.error?.code||result.signal||result.status));
}
function write(relative, source) {
  const target = path.join(app, relative);
  mkdirSync(path.dirname(target), { recursive: true });
  writeFileSync(target, source);
}
function link(source, relative) {
  const target = path.join(app, relative);
  mkdirSync(path.dirname(target), { recursive: true });
  const destination = path.relative(path.dirname(target), source);
  if (existsSync(target)) {
    if (!lstatSync(target).isSymbolicLink() || path.resolve(path.dirname(target), readlinkSync(target)) !== source) throw new Error('Unexpected build link: ' + relative);
    if (readlinkSync(target) === destination) return;
    unlinkSync(target); // Only this builder's verified link, never its source.
  }
  symlinkSync(destination, target);
}
verify();
let completedRecord;
try {
  workspacePath(path.join(root,'.work'),{directory:true});
  mkdirSync(app, { recursive: true });
  workspacePath(app,{directory:true});
  const completed=path.join(app,'.next/ha-complete.json');
  if (existsSync(completed)) {
    if (!lstatSync(completed).isFile() || JSON.parse(readFileSync(completed,'utf8')).kind!=='omniroute-ha-backend') throw new Error('Unexpected build completion record');
    unlinkSync(completed); // Invalidate the preceding artifact before any rebuild.
  }
  // Share library directories by symlink. Only the route directory structure is
  // generated; each handler remains a link to the exact official source file.
  for (const entry of readdirSync(path.join(upstream, 'src'), { withFileTypes: true })) {
    if (entry.name === 'app') continue;
    link(path.join(upstream, 'src', entry.name), path.join('src', entry.name));
  }
  const shells={
    'src/app/layout.js':'export const dynamic = "force-dynamic"; export default function Layout({children}) { return <html><body>{children}</body></html>; }\n',
    'src/app/page.js':'export const dynamic = "force-dynamic"; export default function Page() { return null; }\n',
  };
  if(dashboard)for(const [relative,contents]of Object.entries(shells)) {
    const filename=path.join(app,relative);
    if(existsSync(filename)) {
      if(!lstatSync(filename).isFile()||lstatSync(filename).isSymbolicLink()||readFileSync(filename,'utf8')!==contents)throw new Error('Unexpected generated page shell: '+relative);
      unlinkSync(filename);
    }
  }
  let routes = 0,uiEntries=0;
  function walk(directory) {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const source = path.join(directory, entry.name);
      if (entry.isDirectory()) walk(source);
      else {
        const relative=path.relative(upstream,source),page=/^(page|layout|template|loading|error|global-error|not-found|default)\.[jt]sx?$/.test(entry.name);
        if(!dashboard&&(page||relative.startsWith('src/app/docs/'))) {
          const target=path.join(app,relative);
          if(existsSync(target)) {
            if(!lstatSync(target).isSymbolicLink()||path.resolve(path.dirname(target),readlinkSync(target))!==source)throw new Error('Unexpected excluded UI input: '+relative);
            unlinkSync(target);
          }
          continue;
        }
        link(source, relative);
        if (/^route\.[jt]sx?$/.test(entry.name)) routes++;
        if(page)uiEntries++;
      }
    }
  }
  walk(path.join(upstream, 'src/app'));
  for (const name of ['open-sse', 'packages', 'public','config','bin']) {
    link(path.join(upstream, name), name);
  }
  if(dashboard) {
    for(const name of ['docs','source.config.ts','postcss.config.mjs'])link(path.join(upstream,name),name);
    link(path.join(root,'node_modules'),'node_modules');
  } else for(const [relative,contents]of Object.entries(shells))write(relative,contents);
  // Keep security headers/rewrites and the original UI plugins. Resolve through
  // this symlinked app so generated docs stay here, never under upstream.
  let config = readFileSync(path.join(upstream, 'next.config.mjs'), 'utf8');
  if (createHash('sha256').update(config).digest('hex') !== '4520a0ac70196defb89f445ee810010c9d982fc3c05b57f690574f5fd1245fe5') throw new Error('Backend config pin changed');
  config = config.replace('import createNextIntlPlugin from "next-intl/plugin";', dashboard?'const {default:createNextIntlPlugin}=await import("next-intl/plugin");':'')
    .replace('import { createMDX } from "fumadocs-mdx/next";', dashboard?'import { createMDX } from "fumadocs-mdx/next";':'')
    .replace('const withNextIntl = createNextIntlPlugin("./src/i18n/request.ts");', dashboard?'const withNextIntl = createNextIntlPlugin("./src/i18n/request.ts");':'')
    .replace('const withMDX = createMDX();', dashboard?'const withMDX=config=>createMDX({configPath: join(haAppRoot, "source.config.ts"), outDir: join(haAppRoot, ".source")})(config);':'')
    .replace('export default withMDX(withNextIntl(nextConfig));', `const haConfig=overlay.withMongoOverlay({ ...nextConfig, distDir: ".next", output: undefined, reactCompiler: false, outputFileTracingRoot: haRoot, turbopack: { ...nextConfig.turbopack, root: haRoot }, experimental: { ...nextConfig.experimental, cpus: 1, webpackBuildWorker: false }, webpack(options, context) {
      const result = nextConfig.webpack(options, context);
      result.resolve.alias = { ...result.resolve.alias, '@': join(${dashboard?'haAppRoot':'projectRoot'}, 'src'), '@omniroute/open-sse': join(${dashboard?'haAppRoot':'projectRoot'}, 'open-sse'), '@omniroute/browser-pool': join(${dashboard?'haAppRoot':'projectRoot'}, 'packages/browser-pool/src'), '@omniroute/ui-kit': join(${dashboard?'haAppRoot':'projectRoot'}, 'packages/ui-kit/src') };
      result.resolve.modules = ['node_modules',join(haRoot, 'node_modules')];
      result.resolveLoader.modules = ['node_modules',join(haRoot, 'node_modules')];
      result.resolve.symlinks = false;
      result.resolveLoader.symlinks = false;
      result.plugins.push({ apply(compiler) { compiler.hooks.done.tap('HABackendErrors', stats => {
        for (const error of stats.compilation.errors.slice(0, 25)) {
          console.error(error.message);
          const cause=error.error||error.cause;
          if(cause?.resource)console.error('[HA build] denied resource:',cause.permission,cause.resource);
        }
      }); } });
      return result;
    } });\nexport default ${dashboard?'phase=>phase==="phase-production-server"?withNextIntl(haConfig):withMDX(withNextIntl(haConfig))':'haConfig'};`)
    .replace('const projectRoot = dirname(fileURLToPath(import.meta.url));', 'const projectRoot = join(haRoot, "upstream/omniroute");');
  config = 'import overlay from "../../adapter/webpackOverlay.cjs";\nimport intlCompiler from "../../adapter/intlCompiler.cjs";\nintlCompiler.install();\nconst haRoot = fileURLToPath(new URL("../../", import.meta.url));\nconst haAppRoot = dirname(fileURLToPath(import.meta.url));\n' + config;
  write('next.config.mjs', config);
  link(path.join(upstream, 'scripts'), 'scripts');
  const tsconfig = JSON.parse(readFileSync(path.join(upstream, 'tsconfig.json'), 'utf8'));
  for (const [key, values] of Object.entries(tsconfig.compilerOptions.paths)) tsconfig.compilerOptions.paths[key] = values.map(value => path.join(dashboard?app:upstream, value));
  tsconfig.compilerOptions.incremental = false;
  write('tsconfig.json', JSON.stringify(tsconfig));
  write('package.json', JSON.stringify({ name: 'omniroute-ha-backend', private: true, version: '3.8.53', type: 'module' }));
  console.log('Building ' + routes + ' official HTTP route handlers and '+uiEntries+' UI entries; no source tree copy');
  mkdirSync(path.join(app, 'data'), { recursive: true });
  // Restrict Next and its Node subprocesses to workspace reads and build-directory writes.
  const nodeOptions = [process.env.NODE_OPTIONS || '', '--permission', '--allow-worker', '--allow-child-process', '--allow-addons',
    '--allow-fs-read=' + root.replace(/\/$/, ''), '--allow-fs-write=' + app, '--require=' + path.join(root, 'adapter/buildGuard.cjs')].join(' ');
  const result = spawnSync(process.execPath, ['--require', path.join(root, 'adapter/buildGuard.cjs'), path.join(root, 'node_modules/next/dist/bin/next'), 'build', app, '--webpack'], {
    cwd: app, stdio: 'inherit', env: { ...process.env, NODE_OPTIONS: nodeOptions, PATH: path.dirname(process.execPath) + path.delimiter + process.env.PATH, NEXT_TELEMETRY_DISABLED: '1', NEXT_PRIVATE_OUTPUT_TRACE_ROOT: root, OMNIROUTE_SKIP_STANDALONE: '1', DATA_DIR: path.join(app, 'data'), OMNIROUTE_PLUGINS_DIR: path.join(app, 'data/plugins'), NODE_COMPILE_CACHE: path.join(app, 'cache'), TMPDIR: path.join(app, 'data'), NODE_TEST_CONTEXT: 'isolated-build', OMNIROUTE_DISABLE_BACKGROUND_SERVICES: 'true', OMNIROUTE_HEADLESS: '1', REDIS_URL: '', QDRANT_HOST: '' },
  });
  if (result.error || result.signal || result.status !== 0) throw new Error('Backend build failed: '+(result.error?.code||result.signal||result.status));
  const contract=readFileSync(path.join(app,'.next/server/chunks/omniroute-ha-build.json'));
  completedRecord={kind:'omniroute-ha-backend',routeHandlers:routes,dashboard,uiEntries,
    buildId:readFileSync(path.join(app,'.next/BUILD_ID'),'utf8').trim(),contractSha256:createHash('sha256').update(contract).digest('hex')};
} finally { verify(); }
// A failed final integrity check must leave no valid success marker either.
write('.next/ha-complete.json',JSON.stringify(completedRecord));
