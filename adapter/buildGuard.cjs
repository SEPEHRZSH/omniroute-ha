// Build-only diagnostics and network isolation. Dependencies may probe for
// optional platform files, but the Node permission boundary remains enforced.
process.on('uncaughtExceptionMonitor', error => process.stderr.write(String(error.stack) + '\n'));
const { Socket } = require('node:net');
Socket.prototype.connect = function () { throw new Error('Network access disabled during the HA backend build'); };
const fs = require('node:fs');
const path = require('node:path');
const { fileURLToPath } = require('node:url');
const root = path.resolve(__dirname, '..');
const readlink = fs.readlink;
// Webpack's async resolver probes every ancestor for symlinks. Node's
// permission API throws synchronously for denied probes, while that resolver
// expects a callback error. Deny them without performing any filesystem call.
fs.readlink = function (filename, ...args) {
  const value = filename instanceof URL ? fileURLToPath(filename) : String(filename);
  const absolute = path.resolve(value);
  if (absolute !== root && !absolute.startsWith(root + path.sep)) {
    const callback = args.at(-1);
    const error = Object.assign(new Error('Build readlink outside workspace denied'), { code: 'EACCES', path: value });
    if (typeof callback !== 'function') throw error;
    queueMicrotask(() => callback(error));
    return;
  }
  return readlink.call(this, filename, ...args);
};
// JS realpath probes /home and other ancestors individually, even for an
// allowed workspace request. The native API retains Node's path permission
// check; restrict both its input and resolved output to the workspace too.
const nativeRealpath=fs.realpath.native;
const inside=value=>value===root||value.startsWith(root+path.sep);
function deniedPath(value){return Object.assign(new Error('Build realpath outside workspace denied'),{code:'EACCES',path:value});}
function workspaceRealpath(filename,...args) {
  const value=filename instanceof URL?fileURLToPath(filename):Buffer.isBuffer(filename)?filename.toString():String(filename);
  const absolute=path.resolve(value),callback=args.at(-1);
  if(typeof callback!=='function')throw new TypeError('realpath callback required');
  if(!inside(absolute)){queueMicrotask(()=>callback(deniedPath(absolute)));return;}
  return nativeRealpath.call(fs,filename,...args.slice(0,-1),(error,resolved)=>{
    if(error)return callback(error);
    if(!inside(Buffer.isBuffer(resolved)?resolved.toString():resolved))return callback(deniedPath(value));
    callback(null,resolved);
  });
}
workspaceRealpath.native=workspaceRealpath;fs.realpath=workspaceRealpath;
require('node:module').syncBuiltinESMExports();

require('./intlCompiler.cjs').install();
