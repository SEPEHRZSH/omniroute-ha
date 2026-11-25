// A separate browser only: never attach to an existing session or personal profile.
import {chromium} from 'playwright';
import {mkdtempSync,rmSync} from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';
export async function withIsolatedBrowser(run){
 const executablePath=process.env.HA_TEST_BROWSER_BIN;
 assert.equal(executablePath,'/usr/bin/google-chrome','Set HA_TEST_BROWSER_BIN to the explicitly authorized installed Chrome');
 const directory=mkdtempSync(path.resolve('.work/browser-profile-')),previousTemporary=process.env.TMPDIR;let context;
 process.env.TMPDIR=directory; // Playwright's own transient artifacts stay here too.
 try{
  context=await chromium.launchPersistentContext(directory,{executablePath,headless:true,chromiumSandbox:true,timeout:20000,
   env:{PATH:'/usr/bin:/bin',LANG:'C.UTF-8',TZ:'UTC',TMPDIR:directory,XDG_CONFIG_HOME:directory,XDG_CACHE_HOME:directory,XDG_RUNTIME_DIR:directory},
   args:['--disable-background-networking','--disable-component-update','--disable-sync','--no-first-run']});
  return await run(context);
 }finally{try{await context?.close();}finally{if(previousTemporary===undefined)delete process.env.TMPDIR;else process.env.TMPDIR=previousTemporary;rmSync(directory,{recursive:true});}}
}
export async function verifyBrowserHttp(base,password,phase){
 const origin=new URL(base);assert.equal(origin.hostname,'127.0.0.1');assert.equal(origin.protocol,'http:');assert.ok(origin.port);
 const repetitions=Number(process.env.HA_TEST_BROWSER_REPETITIONS||1);assert.ok(Number.isSafeInteger(repetitions)&&repetitions>=1&&repetitions<=3);
 for(let attempt=1;attempt<=repetitions;attempt++)await withIsolatedBrowser(async context=>{
  await context.route('**/*',route=>new URL(route.request().url()).origin===origin.origin?route.continue():route.abort());
  const page=await context.newPage(),errors=[];page.on('pageerror',error=>errors.push(error.message.slice(0,500)));
  const dataFailures=[],requiredData=new Set(['/api/settings','/api/settings/database','/api/settings/memory','/api/providers','/api/provider-nodes','/api/providers/expiration','/api/provider-metrics','/api/models','/api/usage/call-logs','/api/usage/analytics','/api/memory','/api/memory/embedding-providers','/api/memory/rerank-providers','/api/keys','/api/quota/pools','/api/quota/groups','/api/sessions']);
  page.on('response',response=>{const endpoint=new URL(response.url()).pathname;if(requiredData.has(endpoint)&&response.status()>=500)dataFailures.push({endpoint,status:response.status()});});
  const navigation=[];page.on('response',response=>{if(response.request().isNavigationRequest()){navigation.push({path:new URL(response.url()).pathname,status:response.status()});if(navigation.length>20)navigation.shift();}});
  page.on('console',message=>{if(message.type()==='error'&&/hydration|Minified React error|Uncaught/i.test(message.text()))errors.push(message.text().slice(0,500));});
  let login;
  try{
  const document=await page.goto(base+'/login',{waitUntil:'load'});assert.ok(document);assert.equal(document.status(),200,'Initial browser login document');
  await page.locator('input[type="password"]').fill(password);
  [login]=await Promise.all([
   page.waitForResponse(response=>new URL(response.url()).pathname==='/api/auth/login'&&response.request().method()==='POST'),
   page.locator('form').filter({has:page.locator('input[type="password"]')}).locator('button[type="submit"]').click(),
  ]);
  assert.equal(login.status(),200,login.status()===200?'':await login.text());
  // Upstream /dashboard is a redirect to /home, not the dashboard document.
  await page.waitForURL(url=>url.pathname==='/home',{timeout:20000});}
  catch(error){
   const cookies=await context.cookies(base),state=await context.request.get(base+'/api/settings/require-login',{timeout:5000}).then(response=>response.json()).catch(()=>({unavailable:true}));
   const cookieHeader=login?.headers()['set-cookie']||'';
   console.error('BROWSER_AUTH_DIAGNOSTIC '+JSON.stringify({navigation,authCookiePresent:cookies.some(cookie=>cookie.name==='auth_token'),pendingMfaPresent:cookies.some(cookie=>cookie.name==='mfa_pending'),
    loginSetAuthCookie:/(?:^|,\s*)auth_token=/.test(cookieHeader),loginSecureCookie:/;\s*secure(?:;|$)/i.test(cookieHeader),authenticated:state.authenticated,requireLogin:state.requireLogin,unavailable:state.unavailable}));
   throw error;
  }
  // Dashboard polling intentionally prevents networkidle. Observe each page's
  // actual data request instead of mistaking polling for a broken UI.
  for(const[endpoint,api]of [['/home','/api/usage/call-logs'],['/dashboard/settings/general','/api/settings/database'],['/dashboard/providers','/api/providers'],['/dashboard/api-manager','/api/keys'],['/dashboard/memory','/api/settings/memory']]){
   console.log('BROWSER navigating '+endpoint);
   const analytics=endpoint==='/dashboard/api-manager'?page.waitForResponse(response=>new URL(response.url()).pathname==='/api/usage/analytics'&&new URL(response.url()).searchParams.get('view')==='apiKeys',{timeout:30000}):null;
   const [data,response,usage]=await Promise.all([
    page.waitForResponse(response=>new URL(response.url()).pathname===api&&response.request().method()==='GET',{timeout:30000}),
    page.goto(base+endpoint+'?ha_browser=1',{waitUntil:'load'}),
    analytics,
   ]);
   assert.ok(response);assert.equal(response.status(),200);assert.equal(new URL(page.url()).pathname,endpoint);
   assert.equal(data.status(),200,'Browser data request '+api);await data.finished();
   if(usage){assert.equal(usage.status(),200,'API Manager lifetime usage must load, not silently display zero after a query failure');assert.ok(Array.isArray((await usage.json()).byApiKey));}
   assert.equal(await page.locator('body').innerText().then(text=>text.trim().length>20),true);
   if(endpoint==='/dashboard/memory'){
    await page.getByTestId('tab-engine').click();await page.waitForURL(/tab=engine/);
    await page.getByTestId('tab-memories').click();await page.waitForURL(/tab=memories/);
   }
   console.log('PASS isolated browser page '+endpoint+': hydrated data request '+api);
  }
  assert.deepEqual(errors,[],'Browser hydration/runtime errors');
  assert.deepEqual(dataFailures,[],'Required admin data APIs must succeed, not only render an error shell');
  console.log('PASS isolated browser '+phase+' '+attempt+'/'+repetitions+': real password login, dashboard/settings/providers/Memory hydration, no page errors; Chrome '+context.browser().version());
 });
}
if(process.argv[1]&&import.meta.url===pathToFileURL(path.resolve(process.argv[1])).href){
 assert.equal(process.argv[2],'--probe','Use --probe or import the HTTP helper from an owned fixture');
 await withIsolatedBrowser(async context=>{await context.route('**/*',route=>route.abort());const page=await context.newPage();await page.setContent('<button>Isolated UI probe</button>');await page.getByRole('button',{name:'Isolated UI probe'}).click();console.log('PASS isolated Chrome probe: '+context.browser().version()+', workspace profile and Chromium sandbox enabled');});
}
