/** MongoDB owns the fencing epoch. Redis must never authorize a writer. */
export class ActiveOwnership {
  constructor(db, { leaseMs = 10000, intervalMs = 1000, onPromote, onDemote = () => {} } = {}) {
    if (!onPromote || intervalMs < 50 || intervalMs * 3 >= leaseMs) throw new Error('Invalid ownership lifecycle configuration');
    Object.assign(this, { db, leaseMs, intervalMs, onPromote, onDemote });
    this.state = 'standby';
    this.stopped = false;
  }
  demote(error) {
    const wasActive = this.state === 'active' || this.state === 'promoting';
    this.state = 'standby';
    this.lastError = error?.code || error?.message;
    this.lastCause = error;
    if (wasActive) this.onDemote();
  }
  // Call at the request boundary, not just periodically. A stale health result
  // from a load balancer must not allow a former primary to serve normal traffic.
  ready() {
    if (this.stopped || this.state !== 'active') return false;
    try {
      // The uncached linearizable status read already proves database/quorum
      // reachability. A preceding ping doubles every response-chunk round trip
      // without strengthening ownership. Keep local poison/closed checks too.
      if (this.db.open===false || this.db.compatibilityFault || !this.db.ownership('status')) { this.demote(); return false; }
      return true;
    } catch (error) { this.demote(error); return false; }
  }
  async step() {
    if (this.stopped || this.pending) return;
    this.pending = true;
    try {
      if (!this.db.healthy()) throw new Error('Database is not healthy');
      if (this.state === 'active') this.db.ownership('renew', this.leaseMs);
      else {
        const claim = this.db.ownership('claim', this.leaseMs);
        if (claim) {
          this.state = 'promoting';
          this.epoch = claim.epoch;
          await this.onPromote(claim.epoch);
          if (this.stopped) return;
          // Cache reload may take time. Renew only an unexpired, unchanged epoch.
          this.db.ownership('renew', this.leaseMs);
          this.state = 'active';
        }
      }
    } catch (error) {
      const standby=this.state==='standby';
      this.demote(error);
      // A poisoned worker cannot reconnect. A standby must also be replaced by
      // its supervisor instead of remaining permanently alive but unpromotable.
      if(standby && this.db.open===false) this.onDemote();
    }
    finally { this.pending = false; }
  }
  start() {
    if (this.timer) throw new Error('Ownership loop already started');
    this.stopped = false;
    const loop = async () => {
      await this.step();
      if (!this.stopped) { this.timer = setTimeout(loop, this.intervalMs); this.timer.unref(); }
    };
    void loop();
  }
  stop() {
    this.stopped = true;
    clearTimeout(this.timer);
    this.timer = undefined;
    this.demote();
    try { this.db.ownership('release', this.leaseMs); } catch { /* Lease expires on the server. */ }
  }
}
