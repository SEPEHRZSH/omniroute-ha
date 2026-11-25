# Install and verify OmniRoute HA

The deployment instructions target Ubuntu 24.04 LTS on x86-64 with glibc. Install Git,
Bash, curl, OpenSSL, tar, xz, and unzip. Allow at least 8 GiB of RAM for the
dashboard build and at least 6 GiB of free space for source, dependencies,
build output, and test data. Use Node.js **22.22.2**. Single-host testing used
Ubuntu 22.04, MongoDB **8.0.16**, and Redis **7.2.17**. Qualify other patch
versions against the checks in section 5. Ubuntu 24.04 package installation
and the two-VM procedure still require validation on the target hosts.

Use an unprivileged account for commands that do not specify `sudo`. Extract
into an empty directory whose path contains no spaces. If the checksum file
was supplied, run `sha256sum -c OmniRoute-HA-v3.8.53.zip.sha256` before extraction.

## 1. Source, tools and build on each application server

```sh
unzip OmniRoute-HA-v3.8.53.zip
cd OmniRoute-HA
# Install the pinned runtime in the project directory.
mkdir -p .work/runtime
cd .work/runtime
curl --fail --location -O https://nodejs.org/dist/v22.22.2/node-v22.22.2-linux-x64.tar.xz
printf '%s  %s\n' 88fd1ce767091fd8d4a99fdb2356e98c819f93f3b1f8663853a2dee9b438068a node-v22.22.2-linux-x64.tar.xz | sha256sum -c -
tar -xJf node-v22.22.2-linux-x64.tar.xz
cd ../..
export PATH="$PWD/.work/runtime/node-v22.22.2-linux-x64/bin:$PATH"
node --version
```

Choose a source installation method.

- Run `scripts/ha source` to fetch the official Git tag.
- Run `scripts/ha source --from /absolute/path/to/OmniRoute` to copy the tag from an existing Git checkout.
- Place a complete checkout, including `.git`, at `upstream/omniroute/` and run `scripts/ha source` to verify it.

The official remote is https://github.com/cheaper-inference/OmniRoute.git.
The installer checks tag v3.8.53 and full commit
`191dce8d534ceb56c28a08e71381284a9a87838f`. A mismatched or modified checkout
stops the build before any overlay applies. Source is read-only to the build.
Dependency integrity comes from the included npm lockfile. Keep upstream and dependency
license notices when sharing a built installation.

```sh
scripts/ha deps
scripts/ha build
bash scripts/verify-upstream.sh
```

For offline installation, supply the exact Node archive or runtime and a
complete npm cache.

```sh
HA_NPM_CACHE=/path/to/cache scripts/ha deps --offline
```

The build writes to `.work/backend` and compiles the original dashboard and API
with the pinned overlay. Build in the intended installation directory on each
host. Follow the coordinated upgrade and rollback procedure in section 6.

## 2. Infrastructure options

### Connect to existing services

Use the existing authorized staging MongoDB replica-set URI with authentication,
`replicaSet` and TLS as appropriate. Every **advertised member** and its certificate
name must work from both app hosts. Use three data-bearing voting members with
majority availability. Configure Redis via `.env.example`:

| Mode | Set |
|---|---|
| Direct or managed VIP | `HA_REDIS_MODE=direct`, `REDIS_URL=rediss://cache.example:6379` |
| Sentinel | `HA_REDIS_MODE=sentinel`, `HA_REDIS_MASTER`, `HA_REDIS_NODES` JSON array of Sentinel host/port pairs |
| Cluster | `HA_REDIS_MODE=cluster`, `HA_REDIS_NODES` JSON array of seed host/port pairs |

Set `HA_REDIS_USERNAME` and `HA_REDIS_PASSWORD` for Redis data access. Sentinel
also accepts `HA_REDIS_SENTINEL_USERNAME` and `HA_REDIS_SENTINEL_PASSWORD`.
`HA_REDIS_TLS=true` and `HA_REDIS_SENTINEL_TLS=true` control separate connections.
Supply an approved private CA via `NODE_EXTRA_CA_CERTS` at process startup and
keep certificate verification enabled. Preflight checks MongoDB schema and
discovery plus Redis PING and Pub/Sub. The runtime can start during a temporary
Redis outage and reconnect. MongoDB remains the authority for writes and auth.

### Install native packages if services are unavailable

Only run these administrator commands on approved fresh Ubuntu 24.04 hosts. The
package manager may start default services. Use the connection steps above
for existing installations, or provision separate instances with their own
storage. Preserve existing service files and data. Official references are
the [MongoDB Ubuntu 8.0 guide](https://www.mongodb.com/docs/v8.0/tutorial/install-mongodb-on-ubuntu/)
and [Redis Sentinel guide](https://redis.io/docs/latest/operate/oss_and_stack/management/sentinel/).

```sh
sudo apt-get update
sudo apt-get install ca-certificates curl gnupg git unzip xz-utils openssl haproxy
curl -fsSLo mongodb-server-8.0.asc https://pgp.mongodb.com/server-8.0.asc
gpg --show-keys --fingerprint mongodb-server-8.0.asc
# Compare the fingerprint with the official MongoDB package documentation/policy.
gpg --dearmor --output mongodb-server-8.0.gpg mongodb-server-8.0.asc
sudo install -m 0644 mongodb-server-8.0.gpg /usr/share/keyrings/mongodb-server-8.0.gpg
printf '%s\n' 'deb [arch=amd64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg] https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse' | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list
sudo apt-get update
apt-cache policy mongodb-org redis-server
sudo apt-get install mongodb-org redis-server redis-sentinel
mongod --version
redis-server --version
```

For a **single-server evaluation**, run `deploy/local-infra.sh` in its own terminal
after setting the soft limit with `ulimit -Sn 8192` if needed (the hard limit
must permit it). It checks the required ports, starts three MongoDB processes and three Redis
data processes with three Sentinels on loopback, and initializes the private
`ha_install` replica set. Ctrl-C stops the fixture and retains its data. The loopback fixture assumes a trusted
single-user test server. It uses no passwords and must never bind publicly.

For **shared native infrastructure**, allocate at least three independent failure
domains or use managed infrastructure. The following illustrates initial setup on
dedicated staging hosts. Use private DNS such as `db1.private`, `db2.private`,
`db3.private` and replace the placeholders consistently. Provision TLS and secrets
through the infrastructure team's protected distribution mechanism.

- On each fresh MongoDB host, preserve the distro systemLog, storage and process
  sections in `/etc/mongod.conf`. Set `net.port: 27017`, `net.bindIp:
  127.0.0.1,dbN.private`, `replication.replSetName: omniroute_staging` and
  `security.keyFile: /etc/mongodb/replica.key` in their respective YAML mappings.
  Generate the shared key once with `umask 077; openssl rand -base64 756 >
  replica.key`, transfer securely and install owned by `mongodb`, mode 0400.
  Set `net.tls.mode: requireTLS`, `net.tls.certificateKeyFile` and
  `net.tls.CAFile` when using TLS. Certificates must include the advertised names
  and the name used by local admin connections. Restart only the newly provisioned
  `mongod` units with `sudo systemctl restart mongod`.
- From a local mongosh on the first member, initialize **once on a fresh set**.
  With TLS use `mongosh --host db1.private --tls --tlsCAFile /path/to/ca.pem`
  via a hostname resolving to loopback on that member, or use a certificate with
  a localhost SAN and `--host localhost` for the localhost exception. Verify
  `rs.status()` shows one PRIMARY and two healthy SECONDARY members.

  ```javascript
  rs.initiate({_id:"omniroute_staging",members:[
    {_id:0,host:"db1.private:27017"},
    {_id:1,host:"db2.private:27017"},
    {_id:2,host:"db3.private:27017"}
  ]})
  ```

  On the elected primary use the localhost exception to create the first admin
  (`use admin`, then `db.createUser({user:"haAdmin",pwd:passwordPrompt(),
  roles:["root"]})`). With that admin create a separate application user in admin
  with a role scoped to the selected `ha_test_*` application database. Fresh
  provisioning uses a generated database name and needs a separately authorized
  provisioning credential. The runtime needs collection/index operations as well
  as reads/writes on its database. Never reuse the admin account for the app.
- On three dedicated Redis data hosts restrict `bind` to loopback plus private
  interfaces and retain `protected-mode yes`. Set a common strong `requirepass`
  and matching `masterauth`. On data nodes 2 and 3 configure
  `replicaof redis1.private 6379` for the initial topology. Install TLS and ACL
  policy for the target environment. On each of three Sentinel hosts configure a
  private `bind`, port 26379 and `sentinel monitor omniroute redis1.private
  6379 2` (use a private IP if hostnames are disabled). Add `sentinel auth-pass
  omniroute <data-password>` and appropriate Sentinel client and peer auth.
  Apply to new `redis-server` and `redis-sentinel` systemd services only.
  Sentinel rewrites its config and requires write access. Use `redis-cli` with
  protected credentials to check `INFO replication`, `SENTINEL CKQUORUM
  omniroute` and `SENTINEL get-master-addr-by-name omniroute`. Use approved ACLs,
  TLS and firewall rules before any listener becomes reachable.

Permit MongoDB 27017 among replica members and from both application hosts.
Permit Redis 6379 and Sentinel 26379 among their peers and from the apps as
required. Permit app HTTP only from ingress/admin on 20128 (or the configured
port). Put client TLS at the ingress and protect the private backend hop. Keep
a MongoDB voting majority and Sentinel majority across failures. Redis uses
asynchronous replication, so Redis failover does not certify zero cache loss. Two app VMs alone do not
provide MongoDB/Redis quorum or redundant ingress.

## 3. Two processes on one server

With existing services or the local native fixture running, configure the application:

```sh
scripts/ha render --project "$PWD" --user "$(id -un)" --local yes \
  --primary 127.0.0.1:20128 --standby 127.0.0.1:20129
umask 077
cp -n .work/deploy/host-a.env.example deploy/host-a.env
cp -n .work/deploy/host-b.env.example deploy/host-b.env
mkdir -p .work/data-a .work/data-b
```

Edit both files. Use these endpoints for the local fixture:

```dotenv
HA_MONGO_URI=mongodb://127.0.0.1:47217,127.0.0.1:47218,127.0.0.1:47219/?replicaSet=ha_install
HA_REDIS_MODE=sentinel
HA_REDIS_MASTER=ha-install
HA_REDIS_NODES=[{"host":"127.0.0.1","port":47479},{"host":"127.0.0.1","port":47480},{"host":"127.0.0.1","port":47481}]
```

Set private per-instance `DATA_DIR`, `TMPDIR` and `OMNIROUTE_PLUGINS_DIR`
to absolute paths under `.work/data-a` and `.work/data-b` respectively. Leave the
rendered loopback hosts and ports. Generate four different shared secrets with
`openssl rand -hex 32` for JWT_SECRET, API_KEY_SECRET, STORAGE_ENCRYPTION_KEY and
HA_STATUS_TOKEN. Use identical corresponding secrets in both files and a strong
shared INITIAL_PASSWORD. Change OMNIROUTE_HA_IDENTITY to a unique shared value.
Do not source dotenv files as shell code. Both files must be owned by the service
user and mode 0600.

Provision **once** from an operator terminal. Privately enter the authorized
provisioning URI when prompted, then take the generated database name from output:

```sh
mkdir -p -m 0700 .work/provision
read -rsp 'MongoDB provisioning URI: ' HA_SETUP_URI; printf '\n'
HA_MONGO_URI="$HA_SETUP_URI" DATA_DIR="$PWD/.work/provision" scripts/ha provision
unset HA_SETUP_URI
```

Set the same output `ha_test_stage_*` name in both env files. Switch to the scoped
runtime URI where required. For existing SQLite consult the migration section.
Check both configurations and compare their sharedFingerprint before starting:

```sh
for envfile in deploy/host-a.env deploy/host-b.env; do
  scripts/ha prepare --env "$envfile"
  scripts/ha preflight --env "$envfile"
done
haproxy -c -f .work/deploy/haproxy.cfg
```

In three separate terminals from the project root, export the Node PATH as above
for the app commands and run:

```sh
scripts/ha start --env deploy/host-a.env   # terminal 1
scripts/ha start --env deploy/host-b.env   # terminal 2
haproxy -db -f .work/deploy/haproxy.cfg    # terminal 3 after syntax check
```

Open `http://127.0.0.1:8080/login` through HAProxy and log in with
`INITIAL_PASSWORD`. Follow the manual or automated failover procedure in
section 5. When finished, stop the applications and HAProxy with Ctrl-C in
their respective terminals. Stop the infrastructure fixture last.

## 4. One application per server

Run section 1 on both VMs. Use section 2 for shared infrastructure and the
provisioning command above on **VM A only**. On both VMs, use the same installation
path, service user and real private DNS names in this example:

```sh
scripts/ha render --project "$PWD" --user "$(id -un)" \
  --primary host-a.private:20128 --standby host-b.private:20128
```

On VM A copy `.work/deploy/host-a.env.example` to `deploy/host.env`. On VM B
copy `.work/deploy/host-b.env.example` to the same destination. Use `umask 077`,
preserve existing files, and set the shared
settings, credentials, and database as in section 3. Each VM keeps its own private
runtime files. On each VM run:

```sh
chmod 600 deploy/host.env
scripts/ha prepare --env deploy/host.env
scripts/ha preflight --env deploy/host.env
```

Compare the shared fingerprints. To run in the foreground, execute
`scripts/ha start --env deploy/host.env` on each VM. For a new systemd installation,
have an administrator inspect the rendered unit and confirm that
`/etc/systemd/system/omniroute-ha.service` is unused.

```sh
sudo install -m 0644 .work/deploy/omniroute-ha.service /etc/systemd/system/omniroute-ha.service
sudo systemctl daemon-reload
sudo systemctl enable --now omniroute-ha.service
journalctl -u omniroute-ha.service -n 50
```

The unit expects exact Node in `.work/runtime/node-v22.22.2-linux-x64`. Use
`scripts/ha service --action status` and administrator-approved systemd stop or
restart for that unit. Manage failover by stopping the currently active unit,
waiting for promotion and then starting it again to rejoin. The HAProxy config is
for an ingress host with access to both private app endpoints. Transfer the
generated config and validate with `haproxy -c -f /path/to/haproxy.cfg`. Use the
8080 listener behind an approved TLS gateway, or render with `--tls-cert
/absolute/project/deploy/tls/ingress.pem` for TLS on 8443. Use a dedicated ingress
or a redundant frontend for end-to-end availability. The load balancer health
check must use GET /readyz with status 200, not /livez. Disable request retries
and close sessions to a backend when marked down. Health and /ha/status are
private. Use HTTPS to clients and share ingress abuse controls across failover.

## 5. Acceptance and regression

### Check the topology

Run this command for the single-server configuration.

```sh
scripts/ha topology --env deploy/host-a.env \
  --url http://127.0.0.1:20128 --peer http://127.0.0.1:20129
```

For section 4, use both private application URLs and `deploy/host.env`.
The output should show one Active and one Standby, with the active instance
ID and epoch matching the durable MongoDB record.

### Verify failover in the dashboard

1. Log in through the stable ingress. In **Combos**, create a test Combo
   with distinguishable model entries, a strategy, and nondefault settings.
   Record the name, order and settings of the models, strategy, and runtime
   configuration. Saving the Combo requires no inference requests.
2. Use the topology check to identify the Active, then stop that instance
   in its terminal or through systemd. Leave the Standby and ingress running.
3. Wait for the peer to report Active with a higher epoch. Refresh the browser
   at the same ingress address and check the saved Combo against the record.
4. Restart the former Active and verify that it rejoins as Standby and its
   direct `/readyz` endpoint returns HTTP 503.

### Run the automated failover drill

In the dashboard, create a test API key with the management scope. Set it as
`HA_ACCEPTANCE_API_KEY` in a private, mode-0600 copy of the host environment
file named `deploy/acceptance.env`.

```sh
mkdir -p -m 0700 .work/acceptance
rec=.work/acceptance/drill.json
scripts/ha acceptance capture --env deploy/acceptance.env --record "$rec" \
  --a http://127.0.0.1:20128 --b http://127.0.0.1:20129 --ingress http://127.0.0.1:8080
scripts/ha acceptance watch-promotion --env deploy/acceptance.env --record "$rec"
```

After the command prints `ARMED`, stop the identified Active in its terminal or unit. The watcher
requires a higher epoch, matching MongoDB owner, preserved Settings, API Key,
Memory and Combo through the same ingress, and revocation denial. The Combo
check compares its name, ordered model steps, strategy and full saved runtime
configuration. Restart the former Active after the watcher passes.

```sh
scripts/ha acceptance verify-rejoin --env deploy/acceptance.env --record "$rec"
```

Verify the former Active is Standby, returns 503 on readiness, and the original
Combo is visible from the stable ingress. After verification, remove the records
created by the automated drill.

```sh
scripts/ha acceptance cleanup --env deploy/acceptance.env --record "$rec"
```

The cleanup command removes the drill's API keys, Memory entry, and Combo.
New requests may fail temporarily during promotion.
Never replay an ambiguous POST to an external provider. Keep the private record.
On isolated VMs also drill Redis failover, primary loss and recovery, asymmetric
network loss, quorum loss and in-flight requests. Never inject faults into shared
production infrastructure. A single-host test does not cover those fault domains.

### Run regression tests

With the isolated native fixture running, set `HA_TEST_MONGO_NOFILE` to a
conservative value at or below the fixture's soft file descriptor limit. The
following checks use 8192 and run sequentially:

```sh
HA_TEST_NATIVE_INFRA=1 HA_TEST_MONGO_NOFILE=8192 HA_TEST_REDIS=1 \
  HA_TEST_EXTENDED_QUOTA=1 HA_TEST_STREAMING=1 HA_TEST_VECTOR_HTTP=1 \
  scripts/ha test --full-backend --reservations --data-recovery --mongo-election
HA_TEST_NATIVE_INFRA=1 HA_TEST_MONGO_NOFILE=8192 HA_TEST_HAPROXY_BIN="$(command -v haproxy)" \
  LIBC=glibc node tests/hostDeployment.mjs
```

Tests create unique disposable databases and loopback Redis subprocesses. The
host deployment test uses a private HAProxy listener when given its executable.
Use the election flag only on the disposable replica set. Optional Chrome tests require
explicitly authorized `/usr/bin/google-chrome` and use private headless profiles.
Check that the test command exits with status 0, reports the expected passing
checks, and prints `UPSTREAM_OK` before and after the run.

## 6. Recovery and troubleshooting

If one process fails, allow the durable lease to expire and verify promotion at
ingress before rejoining it. Do not force two writers. Preserve the shared
JWT, API key, storage encryption, and status secrets across restarts. The active process must retire on uncertain
ownership. Unsupported SQL fails closed. Investigate the exact query without
logging identifiers, bindings or provider tokens. Check all replica advertised
addresses, file descriptor limits, Redis ACL/TLS/discovery, private dotenv
permissions, completion marker and journal. Verify private DATA_DIRs contain no
live SQLite files. An upstream shutdown log label mentioning SQLite is not a
storage check.

For SQLite migration, stop all writers and take a consistent SQLite backup
including committed WAL data. Preserve the original and put a copy inside the
workspace. Use the scoped operator URI and explicit fresh target:

```sh
node scripts/data.mjs import-sqlite .work/migration/storage.sqlite ha_test_import_candidate --rebuild-memory
node scripts/data.mjs backup ha_test_import_candidate .work/migration/candidate.bson
node scripts/data.mjs inspect .work/migration/candidate.bson
```

Set HA_MONGO_URI in the invoking shell through your secret manager. Validate
counts/types/constraints, credentials, quota history and original application
behavior before changing ingress. Review legacy financial baselines with
`scripts/quota.mjs` and provider receipts before enabling previously used keys.
Never approve unknown history as zero. Quiescent backups require both instances
stopped. Restoration goes to a separate fresh database with `node
scripts/data.mjs restore FILE NEW_DATABASE`; historical authority cutover
requires reviewed reconciliation (see command options). Preserve original data
and backups. This bounded BSON tool is not a substitute for infrastructure PITR.

For software upgrade or rollback stop both app instances, preserve a quiescent
backup, install both copies of the reviewed release in separate directories and
verify source, lock, overlay, preflight and topology before ingress. In-place
dashboard updates are denied. Only roll back against a demonstrated compatible
database or an explicitly chosen restored target. Preserve acknowledged writes
and reconcile external provider effects. Current limitations include selected
provider/executor and financial policies, representative capacity, organizational
TLS/ACL, native infrastructure installation and independent hosts.
