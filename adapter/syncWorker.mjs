import { parentPort, workerData } from 'node:worker_threads';
import { deserialize, serialize } from 'node:v8';

const handler = import(workerData.moduleURL).then(module => module.createHandler(workerData.config));
// Avoid an unhandled rejection before the first caller arrives.
handler.catch(() => {});
let queue = Promise.resolve();

parentPort.on('message', message => {
  queue = queue.then(async () => {
    const control = new Int32Array(message.control);
    const payload = new Uint8Array(message.payload);
    let response;
    try {
      const execute = await handler;
      response = { value: await execute(deserialize(message.request)) };
    } catch (error) {
      response = { error: {
        message: error.message || 'Database worker failed',
        code: error.code || 'HA_DATABASE_ERROR',
        outcomeUnknown: Boolean(error.outcomeUnknown),
      } };
    }
    let encoded;
    try {
      encoded = serialize(response);
      if (encoded.length > payload.length) throw new Error('Database result exceeds response buffer');
    } catch (error) {
      encoded = serialize({ error: { message: error.message, code: 'HA_RESULT_LIMIT', outcomeUnknown: true } });
    }
    payload.set(encoded);
    Atomics.store(control, 1, encoded.length);
    Atomics.store(control, 0, 1);
    Atomics.notify(control, 0);
  });
});
