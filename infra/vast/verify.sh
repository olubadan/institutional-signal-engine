#!/usr/bin/env bash
set -Eeuo pipefail

readonly REPO_DIR="${REPO_DIR:-/opt/institutional-signal-engine}"
readonly CONFIG_DIR="/etc/institutional-signal-engine"
readonly RUNTIME_ENV="${CONFIG_DIR}/runtime.env"
readonly RUN_PERSISTENCE="${1:-}"

pass() {
  printf '[verify] PASS: %s\n' "$*"
}

fail() {
  printf '[verify] FAIL: %s\n' "$*" >&2
  exit 1
}

[[ ${EUID} -eq 0 ]] || fail "run as root"

# shellcheck source=/dev/null
source /etc/os-release
[[ ${ID} == "ubuntu" ]] || fail "host is not Ubuntu"
case "${VERSION_ID}" in
  22.04 | 24.04) pass "Ubuntu ${VERSION_ID}" ;;
  *) fail "unsupported Ubuntu ${VERSION_ID}" ;;
esac

vcpu=$(nproc)
((vcpu >= 4)) || fail "fewer than 4 visible vCPUs"
pass "${vcpu} visible vCPUs"

memory_kib=$(awk '/MemTotal/ {print $2}' /proc/meminfo)
((memory_kib >= 16 * 1024 * 1024)) || fail "less than 16 GiB visible RAM"
pass "at least 16 GiB visible RAM"

disk_bytes=$(df -B1 --output=size / | tail -1 | tr -d ' ')
((disk_bytes >= 100 * 1000 * 1000 * 1000)) || fail "root disk smaller than 100 GB"
pass "root disk at least 100 GB"

for command_name in docker git gh jq make gcc psql redis-cli shellcheck uv python3.12; do
  command -v "${command_name}" >/dev/null 2>&1 || fail "missing ${command_name}"
done
docker compose version >/dev/null
pass "required command-line tools available"

[[ $(stat -c '%U:%G:%a' "${CONFIG_DIR}/runtime.env.example") == "root:root:600" ]] ||
  fail "runtime environment template ownership/mode is not root:root:600"
[[ $(stat -c '%U:%G:%a' "${RUNTIME_ENV}") == "root:root:600" ]] ||
  fail "runtime environment ownership/mode is not root:root:600"
pass "environment files protected"

python3.12 "${REPO_DIR}/infra/vast/runtime_probe.py" || exit $?

[[ -d "${REPO_DIR}/.git" ]] || fail "repository clone missing"
[[ $(git -C "${REPO_DIR}" branch --show-current) == "feat/signal-only-vertical-slice" ]] ||
  fail "unexpected repository branch"
pass "repository and Phase 3 branch present"

systemctl is-enabled docker >/dev/null
systemctl is-active docker >/dev/null
systemctl is-enabled institutional-signal-dependencies.service >/dev/null
systemctl is-active institutional-signal-dependencies.service >/dev/null
pass "Docker and dependency service enabled and active"

wait_healthy() {
  local container=$1
  local attempt
  for ((attempt = 0; attempt < 60; attempt++)); do
    if [[ $(docker inspect --format '{{.State.Health.Status}}' "${container}" 2>/dev/null || true) == "healthy" ]]; then
      return 0
    fi
    sleep 2
  done
  return 1
}

wait_healthy institutional-signal-postgres || fail "PostgreSQL container unhealthy"
wait_healthy institutional-signal-redis || fail "Redis container unhealthy"
pass "PostgreSQL and Redis containers healthy"

pass "local dependency connectivity verified by Python data-only probe"

if [[ ${RUN_PERSISTENCE} == "--persistence" ]]; then
  python3.12 "${REPO_DIR}/infra/vast/runtime_probe.py" --persistence || exit $?
fi

sshd_effective_config=$(sshd -T)
grep -qx 'passwordauthentication no' <<<"${sshd_effective_config}" ||
  fail "SSH password authentication is enabled"
grep -Eq '^permitrootlogin (without-password|prohibit-password)$' <<<"${sshd_effective_config}" ||
  fail "root SSH permits password login"
pass "SSH key-only policy active"
pass "Phase 3 environment verification complete"
