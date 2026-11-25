import { fixturePorts } from './mongoTestSupport.mjs';
// Test-only SOCKS5 proxy. Its only possible destinations are the approved local
// disposable replica-set members. It never changes the MongoDB servers.
import net from 'node:net';
import { BSON } from 'mongodb';

export async function createHandler(config) {
  const sockets = new Set();
  let blocked = false, suppressCommit = false, suppressedCommitOK, abortCommand;
  const remember = socket => {
    sockets.add(socket); socket.on('close', () => sockets.delete(socket));
    socket.on('error', () => socket.destroy()); return socket;
  };
  function disconnect() { blocked = true; for (const socket of sockets) socket.destroy(); }
  const server = net.createServer(socket => {
    remember(socket);
    if (blocked) { socket.destroy(); return; }
    let stage = 'greeting', buffer = Buffer.alloc(0), remote, responseBuffer = Buffer.alloc(0), commitId;
    socket.on('close', () => remote?.destroy());
    socket.on('data', chunk => {
      buffer = Buffer.concat([buffer, chunk]);
      if (buffer.length > 1024 * 1024) { socket.destroy(); return; }
      if (stage === 'greeting') {
        if (buffer.length < 2 || buffer.length < 2 + buffer[1]) return;
        if (buffer[0] !== 5 || !buffer.subarray(2, 2 + buffer[1]).includes(0)) { socket.destroy(); return; }
        buffer = buffer.subarray(2 + buffer[1]); socket.write(Buffer.from([5, 0])); stage = 'connect';
      }
      if (stage === 'connect') {
        if (buffer.length < 5) return;
        const addressLength = buffer[3] === 1 ? 4 : buffer[3] === 3 ? buffer[4] : -1;
        const addressStart = buffer[3] === 3 ? 5 : 4;
        if (addressLength < 0 || buffer[0] !== 5 || buffer[1] !== 1) { socket.destroy(); return; }
        const length = addressStart + addressLength + 2;
        if (buffer.length < length) return;
        const host = buffer[3] === 1 ? [...buffer.subarray(4, 8)].join('.') : buffer.subarray(5, 5 + addressLength).toString();
        const port = buffer.readUInt16BE(length - 2);
        if (!['127.0.0.1', 'localhost'].includes(host) || !fixturePorts.includes(port)) { socket.destroy(); return; }
        buffer = buffer.subarray(length); stage = 'connecting';
        remote = remember(net.connect({ host: '127.0.0.1', port }, () => {
          stage = 'wire'; socket.write(Buffer.from([5, 0, 0, 1, 127, 0, 0, 1, 0, 0])); forwardRequests();
        }));
        remote.on('close', () => socket.destroy());
        remote.on('data', data => {
          responseBuffer = Buffer.concat([responseBuffer, data]);
          while (responseBuffer.length >= 16) {
            const length = responseBuffer.readInt32LE(0);
            if (length < 16 || length > 1024 * 1024) { socket.destroy(); return; }
            if (responseBuffer.length < length) return;
            const frame = responseBuffer.subarray(0, length); responseBuffer = responseBuffer.subarray(length);
            if (commitId !== undefined && frame.readInt32LE(8) === commitId) {
              // Observe the successful server reply, then lose it on the wire.
              if (frame.readInt32LE(12) !== 2013 || frame[20] !== 0) throw new Error('Unexpected commit response framing');
              suppressedCommitOK = BSON.deserialize(frame.subarray(21)).ok;
              disconnect(); return;
            }
            socket.write(frame);
          }
        });
      }
      if (stage === 'wire') forwardRequests();
    });
    function forwardRequests() {
      while (buffer.length >= 16) {
        const length = buffer.readInt32LE(0);
        if (length < 16 || length > 1024 * 1024) { socket.destroy(); return; }
        if (buffer.length < length) return;
        const frame = buffer.subarray(0, length); buffer = buffer.subarray(length);
        if (frame.readInt32LE(12) === 2013 && frame[20] === 0) {
          // Deserialize only this test client's wire frame; never log credentials/data.
          const documentSize = frame.readInt32LE(21);
          const command = BSON.deserialize(frame.subarray(21, 21 + documentSize), { promoteLongs: false });
          // Save only this fixture's own transaction identity, for explicit
          // harness cleanup after simulating a client that cannot send abort.
          if (command.$db === config.database && command.autocommit === false && command.lsid && command.txnNumber != null) {
            abortCommand = BSON.serialize({ abortTransaction: 1, lsid: command.lsid, txnNumber: command.txnNumber, autocommit: false });
          }
          if (suppressCommit && command.commitTransaction === 1) { commitId = frame.readInt32LE(4); suppressCommit = false; }
        }
        remote.write(frame);
      }
    }
  });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  return request => {
    if (request.operation === 'port') return server.address().port;
    if (request.operation === 'drop') { disconnect(); return; }
    if (request.operation === 'recover') { blocked = false; return; }
    if (request.operation === 'suppressCommit') { suppressCommit = true; return; }
    if (request.operation === 'status') return { blocked, suppressedCommitOK };
    if (request.operation === 'abortCommand') return abortCommand;
    throw new Error('Unknown fault injection');
  };
}
