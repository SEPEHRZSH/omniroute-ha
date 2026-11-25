export function createHandler() {
  return async request => {
    if (request.operation === 'error') throw Object.assign(new Error('Synthetic failure'), { code: 'TEST_FAILURE' });
    if (request.operation === 'crash') process.exit(2);
    if (request.operation === 'wait') await new Promise(resolve => setTimeout(resolve, request.ms));
    return request.value;
  };
}
