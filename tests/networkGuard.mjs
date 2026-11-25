import { fixturePorts } from './mongoTestSupport.mjs';
import { isMainThread } from 'node:worker_threads';
import { Socket } from 'node:net';

// Keep database I/O in the MongoDB worker and restrict network access to test fixtures.
if (isMainThread) {
  const redisPorts=process.env.HA_TEST_REDIS_PORTS?JSON.parse(process.env.HA_TEST_REDIS_PORTS):[];
  if(!Array.isArray(redisPorts)||redisPorts.length>6||redisPorts.some(port=>!Number.isInteger(port)||port<1||port>65535))throw new Error('Invalid isolated Redis test ports');
  if(process.env.HA_TEST_OAUTH_REDIRECT==='1') {
    const nativeFetch=globalThis.fetch;
    const port=Number(process.env.HA_TEST_PROVIDER_PORT);
    if(!Number.isInteger(port) || port<1 || port>65535) throw new Error('Invalid isolated OAuth test port');
    // Route upstream OAuth requests to the local HTTP fixture.
    const targets=new Map([['https://openference.com/oauth/token','/oauth/token'],['https://api.anthropic.com/v1/oauth/token','/oauth/claude'],['https://auth.openai.com/oauth/token','/oauth/codex'],['https://api.cline.bot/api/v1/auth/refresh','/oauth/cline'],['https://auth.kimi.com/api/oauth/token','/oauth/kimi-coding'],['https://synthetic-gitlab.invalid/oauth/token','/oauth/gitlab-duo'],['https://oidc.us-east-1.amazonaws.com/token','/oauth/kiro'],['https://oidc.us-east-1.amazonaws.com/client/register','/oauth/kiro-register'],['https://login.microsoftonline.com/synthetic-tenant/oauth2/v2.0/token','/oauth/amazon-q'],['https://github.com/login/oauth/access_token','/oauth/github']]);
    targets.set('https://api.github.com/copilot_internal/v2/token','/oauth/github-copilot');
    globalThis.fetch=(input,options)=>{
      const path=(typeof input==='string'||input instanceof URL)&&targets.get(String(input));
      return nativeFetch(path?'http://127.0.0.1:'+port+path:input,options);
    };
  }
  const connect = Socket.prototype.connect;
  Socket.prototype.connect = function (...args) {
    const options = Array.isArray(args[0]) ? args[0][0] : args[0];
    // The independent service launcher performs an explicitly read-only Mongo
    // preflight in its parent. Its application child retains the worker-only
    // rule below; all external/provider traffic remains blocked in both.
    if(process.env.NODE_TEST_CONTEXT==='isolated-host-entry'&&process.argv[1]?.endsWith('/scripts/host.mjs')&&
      options&&['127.0.0.1','localhost'].includes(options.host)&&fixturePorts.includes(Number(options.port)))return connect.apply(this,args);
    if (options && typeof options === 'object' && options.host === '127.0.0.1' &&
      (Number(options.port) === Number(process.env.HA_TEST_PROVIDER_PORT)||redisPorts.includes(Number(options.port)))) return connect.apply(this, args);
    throw new Error('External connection disabled in HA HTTP test');
  };
}
