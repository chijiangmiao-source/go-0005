#!/usr/bin/env bash
set -euo pipefail

tmpdir="$(mktemp -d ./.benzhi-smoke.XXXXXX)"
cache_root="$(cd "${tmpdir}" && (pwd -W 2>/dev/null || pwd))"
export GOCACHE="${cache_root}/go-build-cache"
pid=""
cleanup() {
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    kill "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
  fi
  rm -rf "${tmpdir}"
}
trap cleanup EXIT

go build -o "${tmpdir}/orchestrator" ./cmd/server
"${tmpdir}/orchestrator" -addr 127.0.0.1:18080 -db "${tmpdir}/state.db" &
pid="$!"

ready=""
for _ in {1..50}; do
  if ready="$(curl -fsS http://127.0.0.1:18080/health/ready 2>/dev/null)"; then
    break
  fi
  sleep 0.1
done

if [[ "${ready}" != *'"ready":true'* ]]; then
  echo "readiness check failed: ${ready}" >&2
  exit 1
fi

resources="$(curl -fsS http://127.0.0.1:18080/api/v1/resources)"
if [[ "${resources}" != *'"antenna"'* ]]; then
  echo "resource probe failed: ${resources}" >&2
  exit 1
fi

page="$(curl -fsS http://127.0.0.1:18080/)"
if [[ "${page}" != *'试验窗口编排控制台'* ]]; then
  echo "frontend probe failed" >&2
  exit 1
fi

echo "benzhi smoke ok"
