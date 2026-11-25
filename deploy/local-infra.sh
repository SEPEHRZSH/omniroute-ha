#!/usr/bin/env bash
# Loopback-only staging fixture. Ctrl-C stops the processes and retains their data.
set -Eeuo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
umask 077
die() { printf '%s\n' "$*" >&2; exit 1; }
[[ "$(id -u)" != 0 ]] || die 'Run this fixture as an ordinary user.'
for tool in mongod mongosh redis-server redis-cli flock timeout; do
  command -v "$tool" >/dev/null || die "Install the required command first: $tool"
done
[[ "$(ulimit -n)" -ge 8192 ]] || die 'Set the fixture soft nofile limit to at least 8192 in this shell.'
[[ ! -L .work && ! -L .work/local-infra ]] || die 'Refusing redirected infrastructure storage.'
mkdir -p .work/local-infra
root="$PWD/.work/local-infra"
exec 9>"$root/lock"
flock -n 9 || die 'This local infrastructure is already running.'
for port in 47217 47218 47219 47379 47380 47381 47479 47480 47481; do
  if timeout 1 bash -c 'true </dev/tcp/127.0.0.1/"$1"' bash "$port" 2>/dev/null; then
    die "Port $port is occupied. No processes were started."
  fi
done
children=()
cleanup() {
  trap - EXIT
  if (( ${#children[@]} )); then
    kill -TERM "${children[@]}" 2>/dev/null || true
    wait "${children[@]}" 2>/dev/null || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
start() { "$@" 9>&- & children+=("$!"); }
mongo() { mongosh 'mongodb://127.0.0.1:47217/?directConnection=true&serverSelectionTimeoutMS=1500' --quiet --eval "$1"; }
retry() {
  for (( attempt=0; attempt<30; attempt++ )); do
    if "$@" >/dev/null 2>&1; then return; fi
    sleep 1
  done
  die "Infrastructure did not become ready. Inspect private logs in $root."
}
for port in 47217 47218 47219; do
  [[ ! -L "$root/mongo-$port" && ! -L "$root/mongo-$port.log" ]] || die 'Refusing redirected MongoDB data/log.'
  mkdir -p "$root/mongo-$port"
  start mongod --dbpath "$root/mongo-$port" --logpath "$root/mongo-$port.log" --logappend \
    --bind_ip 127.0.0.1 --port "$port" --replSet ha_install --wiredTigerCacheSizeGB 0.25
done
retry mongo 'if (!db.adminCommand({ping:1}).ok) quit(1)'
mongo '
const members = [47217,47218,47219].map((p,_id)=>({_id,host:"localhost:"+p}));
try {
  const c=rs.conf();
  if(c._id!=="ha_install" || c.members.map(x=>x.host).sort().join()!==members.map(x=>x.host).join()) quit(1);
} catch(e) {
  if(e.code!==94) throw e;
  if(!rs.initiate({_id:"ha_install",members}).ok) quit(1);
}'
retry mongo 'let m=rs.status().members; if(m.filter(x=>x.health===1&&x.state===1).length!==1 || m.filter(x=>x.health===1&&x.state===2).length!==2) quit(1)'
for port in 47379 47380 47381; do
  [[ ! -L "$root/redis-$port.log" ]] || die 'Refusing redirected Redis log.'
  args=()
  if [[ "$port" != 47379 ]]; then args=(--replicaof 127.0.0.1 47379); fi
  start redis-server --bind 127.0.0.1 --protected-mode yes --port "$port" \
    --dir "$root" --save '' --appendonly no --logfile "$root/redis-$port.log" "${args[@]}"
done
for port in 47479 47480 47481; do
  cfg=$(mktemp "$root/sentinel-$port.XXXXXXXX")
  cat >"$cfg" <<CFG
bind 127.0.0.1
protected-mode yes
port $port
sentinel monitor ha-install 127.0.0.1 47379 2
sentinel down-after-milliseconds ha-install 3000
sentinel failover-timeout ha-install 15000
CFG
  start redis-server "$cfg" --sentinel
done
redis_ready() {
  redis-cli -p 47479 sentinel ckquorum ha-install | awk '/^OK / { ok=1 } END {exit !ok}' &&
    redis-cli -p 47379 info replication | awk '/^connected_slaves:2/ { ok=1 } END {exit !ok}'
}
retry redis_ready
printf '%s\n' 'READY: MongoDB ha_install (47217–47219), Redis Sentinel ha-install (47479–47481). Keep this terminal open.'
wait -n "${children[@]}"
die 'An owned infrastructure process exited. The fixture is stopping its other children.'
