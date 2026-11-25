import {AsyncLocalStorage} from 'node:async_hooks';
import {randomUUID} from 'node:crypto';
// Pricing yields before the official synchronous insert transaction. Bind the
// preview to that transaction, without holding a DB transaction across await.
export function createRetentionRuntime(db,runtime) {
  const scope=new AsyncLocalStorage(),mode=new AsyncLocalStorage();let busy=false;
  db.usageRollupScope=()=>scope.getStore()?.id;
  db.usagePurgeScope=()=>scope.getStore()?.purge===true;
  const retention={
    prices(rows) {const current=scope.getStore();if(!current)throw new Error('Rollup pricing context required');db.bridge.call({operation:'recordUsagePrices',id:current.id,rows});},
    finish(before) {
      const current=scope.getStore();if(!current||!db.inTransaction)throw new Error('Rollup transaction required');
      current.finished=db.bridge.call({operation:'finishUsageRollup',id:current.id,before});
      if(current.finished.deleted)db.changedTables.add('usage_history');
    },
    async purgeBefore(before) {
      return mode.run(true,async()=>{const {rollupUsageHistoryBeforeDate}=await import('../upstream/omniroute/src/lib/usage/aggregateHistory.ts');return rollupUsageHistoryBeforeDate(before);});
    },
    async cleanupUsageHistory() {
      runtime.assertOwner();
      const {getUserDatabaseSettings}=await import('../upstream/omniroute/src/lib/db/databaseSettings.ts');
      const days=getUserDatabaseSettings().retention.usageHistory;
      if(!Number.isSafeInteger(days)||days<1)throw new Error('Invalid usage history retention');
      const cutoff=new Date(db.serverTime());cutoff.setDate(cutoff.getDate()-days);
      const result=await retention.purgeBefore(cutoff.toISOString().split('T')[0]);
      return {deleted:result.errors?0:result.deletedRows,errors:result.errors,hasMore:result.hasMore};
    },
    async run(fn) {
    runtime.assertOwner();
    if(busy||db.inTransaction)throw Object.assign(new Error('Usage rollup already running or inside a transaction'),{code:'HA_ROLLUP_BUSY'});
    busy=true;const id=randomUUID(),current={id,purge:mode.getStore()===true};
    try {
      const {invalidateDbCache}=await import('../upstream/omniroute/src/lib/db/readCache.ts');
      invalidateDbCache('pricing');
      for(const invalidate of globalThis.__omnirouteReadInvalidators??[])invalidate('pricing');
      runtime.assertOwner();
      const result=await scope.run(current,fn);runtime.assertOwner();
      if(current.purge&&result?.errors===0)return {...result,deletedRows:current.finished?.deleted??0,hasMore:current.finished?.hasMore??false};
      return result;
    } finally {
      busy=false;
      if(db.open)try{db.bridge.call({operation:'releaseUsageRollup',id});}catch{/* A poisoned/closed worker cannot replay this preview. */}
    }
  }};return retention;
}
