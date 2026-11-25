import { runtimeRequire } from '../../../../../upstream/omniroute/src/lib/db/adapters/runtimeRequire.ts';

// Check the compiled DB-driver boundary without opening SQLite. A harmless
// builtin would resolve successfully if the build omitted the overlay.
function verifyCompiledDriver() {
  try { runtimeRequire('node:path'); }
  catch (error) {
    if (error.message === 'SQLite driver loading is forbidden in MongoDB mode: node:path') return;
    throw error;
  }
  throw new Error('The Next.js build did not include the MongoDB driver overlay');
}

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

async function dispatch(request) {
  verifyCompiledDriver();
  if (!globalThis.__omnirouteRouteTestDispatch) return Response.json({ error: 'Test runtime unavailable' }, { status: 503 });
  return globalThis.__omnirouteRouteTestDispatch(request);
}
export { dispatch as GET, dispatch as POST, dispatch as PUT, dispatch as PATCH, dispatch as DELETE };
