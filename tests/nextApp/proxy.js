// Exercise the upstream authorization pipeline through the Next.js proxy.
export async function proxy(request) {
  if (!globalThis.__omnirouteTestProxy) return Response.json({ error: 'HA proxy is not initialized' }, { status: 503 });
  return globalThis.__omnirouteTestProxy(request);
}
export const config = { matcher: ['/api/:path*'] };
