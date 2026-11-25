// Reuse the original sync cycle/providers. The active application's MongoDB
// epoch fences every catalog/error/timestamp write; Redis never grants a run.
export function createFreeProxySync(runtime) {
  let pending=false,closed=false,timer;
  const state={started:false,runs:0};
  const assertOwner=()=>{if(closed)throw new Error('Free Proxy sync is stopped');runtime.assertOwner();};
  async function run(providers,original) {
    assertOwner();
    if(pending)throw Object.assign(new Error('Free Proxy sync already running'),{code:'HA_FREE_PROXY_SYNC_BUSY',status:409});
    pending=true;
    try {
      const selected=providers??(await import('../upstream/omniroute/src/lib/freeProxyProviders/index.ts')).getEnabledProviders();
      assertOwner();
      const guarded=selected.map(provider=>({id:provider.id,async sync(){
        assertOwner();const result=await provider.sync();assertOwner();return result;
      }}));
      const result=await original(guarded);assertOwner();
      state.runs++;state.lastSyncAt=result.lastSyncAt;
      return result;
    } finally {pending=false;}
  }
  async function start() {
    assertOwner();
    if(process.env.FREE_PROXY_AUTO_SYNC_ENABLED!=='true')return;
    // Importing the original scheduler must not arm its independent timers.
    if(!['1','true','yes','on'].includes((process.env.OMNIROUTE_DISABLE_BACKGROUND_SERVICES||'').trim().toLowerCase()))throw new Error('HA Free Proxy scheduler requires the controlled background profile');
    const [{getFreeProxyAutoSyncIntervalMs},{getEnabledProviders},{getFreeProxyStats}]=await Promise.all([
      import('../upstream/omniroute/src/lib/freeProxyProviders/scheduler.ts'),
      import('../upstream/omniroute/src/lib/freeProxyProviders/index.ts'),
      import('../upstream/omniroute/src/lib/db/freeProxies.ts'),
    ]);
    assertOwner();if(!getEnabledProviders().length)return;
    const interval=getFreeProxyAutoSyncIntervalMs(),{lastSyncAt}=await getFreeProxyStats();assertOwner();
    const last=Date.parse(lastSyncAt),elapsed=runtime.db.serverTime()-last;
    const initial=!Number.isFinite(last)||elapsed>=interval?5000:Math.max(interval-elapsed,5000);
    const tick=async()=>{
      try {
        assertOwner();
        const {runFreeProxySyncCycle}=await import('../upstream/omniroute/src/lib/freeProxyProviders/syncCycle.ts');
        await runFreeProxySyncCycle();assertOwner();
      } catch(error) {
        if(closed)return;
        if(!runtime.ready()){runtime.retire(error.code||'Free Proxy sync ownership unavailable');return;}
        // Source HTTP errors remain source-scoped in the original cycle. A
        // manual cycle may be in flight; never overlap it or replay its request.
        console.warn('[HA] Free Proxy sync deferred:',String(error.code||'HA_FREE_PROXY_SYNC_FAILED'));
      }
      if(!closed){timer=setTimeout(tick,interval);timer.unref();}
    };
    state.started=true;timer=setTimeout(tick,Math.min(initial,2147483647));timer.unref();
  }
  return {run,start,state,stop(){closed=true;state.started=false;clearTimeout(timer);}};
}
