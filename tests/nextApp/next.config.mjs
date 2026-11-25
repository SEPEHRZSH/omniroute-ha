import loader from '../../adapter/webpackOverlay.cjs';

// A single build shared by both peers; source inputs remain in the upstream checkout.
export default loader.withMongoOverlay({
  poweredByHeader: false,
  experimental: { cpus: 1 },
});
