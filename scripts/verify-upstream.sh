#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
UP="$ROOT/upstream/omniroute"
[[ -d "$UP/.git" ]] || { echo 'Upstream checkout missing' >&2; exit 1; }
[[ "$(git -C "$UP" describe --tags --exact-match HEAD 2>/dev/null)" == 'v3.8.53' ]] || { echo 'Unexpected upstream tag' >&2; exit 1; }
[[ "$(git -C "$UP" rev-parse HEAD)" == '191dce8d534ceb56c28a08e71381284a9a87838f' ]] || { echo 'Unexpected upstream commit' >&2; exit 1; }
[[ -z "$(git -C "$UP" status --porcelain --untracked-files=all)" ]] || { echo 'Upstream is modified' >&2; exit 1; }
echo 'UPSTREAM_OK: v3.8.53 @ 191dce8d534c (clean)'
