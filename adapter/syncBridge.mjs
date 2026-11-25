import { Worker } from 'node:worker_threads';
import { deserialize, serialize } from 'node:v8';

/** Synchronous caller contract; all network I/O runs on a dedicated worker. */
export class SyncBridge {
  constructor(moduleURL, options = {}) {
    this.timeoutMs = options.timeoutMs ?? 5000;
    this.responseBytes = options.responseBytes ?? 8 * 1024 * 1024;
    if (!Number.isSafeInteger(this.timeoutMs) || this.timeoutMs < 1) {
      throw new RangeError('timeoutMs must be a positive integer');
    }
    if (!Number.isSafeInteger(this.responseBytes) || this.responseBytes < 1024) {
      throw new RangeError('responseBytes must be at least 1024');
    }
    // Calls are synchronous and serialized. Reuse the result capacity instead
    // of allocating megabytes for every small SQL statement during bootstrap.
    // Each call still needs its own completion cell: a late notify from the
    // preceding worker callback must never complete the next request.
    this.payload = new Uint8Array(new SharedArrayBuffer(this.responseBytes));
    this.open = true;
    this.worker = new Worker(new URL('./syncWorker.mjs', import.meta.url), {
      workerData: { moduleURL: String(moduleURL), config: options.config ?? {} },
    });
    // Worker events cannot run while the main thread is in Atomics.wait.
    // The finite deadline below is also the worker-crash detection boundary.
    this.worker.on('error', () => { this.open = false; });
    this.worker.on('exit', () => { this.open = false; });
    this.worker.unref();
  }

  call(request) {
    if (!this.open) throw Object.assign(new Error('Database bridge is closed'), { code: 'HA_BRIDGE_CLOSED' });
    const control = new Int32Array(new SharedArrayBuffer(8));
    const payload = this.payload;
    this.worker.postMessage({ request: serialize(request), control: control.buffer, payload: payload.buffer });
    const result = Atomics.wait(control, 0, 0, this.timeoutMs);
    if (result === 'timed-out') {
      this.close();
      throw Object.assign(new Error('Database operation timed out; outcome must be reconciled before retry'), {
        code: 'HA_BRIDGE_TIMEOUT', outcomeUnknown: true,
      });
    }
    const length = Atomics.load(control, 1);
    if (length <= 0 || length > payload.byteLength) {
      this.close();
      throw Object.assign(new Error('Invalid database worker response'), { code: 'HA_BRIDGE_PROTOCOL', outcomeUnknown: true });
    }
    const response = deserialize(Buffer.from(payload.subarray(0, length)));
    if (response.error) {
      const error = Object.assign(new Error(response.error.message), response.error);
      if (error.outcomeUnknown) this.close();
      throw error;
    }
    return response.value;
  }

  close() {
    this.open = false;
    return this.worker.terminate();
  }
}
