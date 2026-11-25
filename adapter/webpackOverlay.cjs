const path = require('node:path');
const { realpathSync } = require('node:fs');
const root = path.resolve(__dirname, '../upstream/omniroute');
function canonical(filename) {
  try { return realpathSync(filename); } catch { return filename; }
}

module.exports = function checkedOverlay(source) {
  const { overlay } = require('./overlay.mjs');
  this.addDependency(require.resolve('./overlay.mjs'));
  const filename = path.relative(root, canonical(this.resourcePath)).split(path.sep).join('/');
  const result = overlay(filename, source);
  this._module.buildInfo.omnirouteOverlay = filename;
  return result;
};

// Apply before Next's TS/SWC loaders. A changed upstream digest is a build
// error. Keep the original project's webpack callback and loader rules intact.
module.exports.withMongoOverlay = function withMongoOverlay(config) {
  const previous = config.webpack;
  return { ...config, webpack(options, context) {
    // Next also reads this config at server startup, under the Node source
    // hook. Only the build needs the ESM transform and its pin table.
    const { overlayPins } = require('./overlay.mjs');
    const files = new Set(Object.keys(overlayPins).map(filename => path.join(root, filename)));
    const result = previous ? previous.call(this, options, context) : options;
    result.module.rules.unshift({ enforce: 'pre', test: filename => files.has(canonical(filename)), use: [__filename] });
    if (context.isServer && context.nextRuntime !== 'edge') result.plugins.push({ apply(compiler) {
      compiler.hooks.thisCompilation.tap('OmniRouteHAContract', compilation => {
        compilation.hooks.processAssets.tap({ name: 'OmniRouteHAContract', stage: context.webpack.Compilation.PROCESS_ASSETS_STAGE_REPORT }, () => {
          const applied = new Set();
          const inspect = module => {
            if (module.buildInfo?.omnirouteOverlay) applied.add(module.buildInfo.omnirouteOverlay);
            for (const child of module.modules || []) inspect(child);
          };
          for (const module of compilation.modules) inspect(module);
          const { overlayFingerprint } = require('./buildContract.mjs');
          const contract = { upstream: '191dce8d534c', overlay: overlayFingerprint(), pins: overlayPins, applied: [...applied].sort() };
          compilation.emitAsset('omniroute-ha-build.json', new context.webpack.sources.RawSource(JSON.stringify(contract)));
        });
      });
    } });
    return result;
  } };
};
