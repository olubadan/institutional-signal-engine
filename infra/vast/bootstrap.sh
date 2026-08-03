#!/usr/bin/env bash
set -Eeuo pipefail

umask 027

readonly REPO_DIR="${REPO_DIR:-/opt/institutional-signal-engine}"
readonly CONFIG_DIR="/etc/institutional-signal-engine"
readonly STATE_DIR="/var/lib/institutional-signal-engine"
readonly RUNTIME_ENV="${CONFIG_DIR}/runtime.env"
readonly RUNTIME_TEMPLATE="${CONFIG_DIR}/runtime.env.example"
readonly UV_VERSION="0.11.28"
readonly UV_PYTHON_INSTALL_DIR="/opt/uv/python"

log() {
  printf '[bootstrap] %s\n' "$*"
}

fail() {
  printf '[bootstrap] ERROR: %s\n' "$*" >&2
  exit 1
}

[[ ${EUID} -eq 0 ]] || fail "run as root"
[[ -f "${REPO_DIR}/infra/vast/compose.yaml" ]] || fail "repository not found at ${REPO_DIR}"

# shellcheck source=/dev/null
source /etc/os-release
[[ ${ID} == "ubuntu" ]] || fail "Ubuntu is required"
case "${VERSION_ID}" in
  22.04 | 24.04) ;;
  *) fail "supported Ubuntu releases are 22.04 and 24.04" ;;
esac

export DEBIAN_FRONTEND=noninteractive
log "installing system packages"
apt-get update -qq
apt-get install -y --no-install-recommends \
  build-essential \
  ca-certificates \
  curl \
  gh \
  git \
  jq \
  libpq-dev \
  openssh-client \
  openssh-server \
  openssl \
  pkg-config \
  postgresql-client \
  redis-tools \
  rsync \
  shellcheck \
  unattended-upgrades \
  unzip

if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  log "installing Docker Engine and Compose from Docker's Ubuntu repository"
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  printf 'deb [arch=%s signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu %s stable\n' \
    "$(dpkg --print-architecture)" "${VERSION_CODENAME}" \
    >/etc/apt/sources.list.d/docker.list
  apt-get update -qq
  apt-get install -y --no-install-recommends \
    containerd.io docker-buildx-plugin docker-ce docker-ce-cli docker-compose-plugin
fi

if ! command -v uv >/dev/null 2>&1 || [[ $(uv --version | awk '{print $2}') != "${UV_VERSION}" ]]; then
  log "installing pinned uv ${UV_VERSION}"
  install_script=$(mktemp)
  trap 'rm -f "${install_script}"' EXIT
  curl -LsSf "https://astral.sh/uv/${UV_VERSION}/install.sh" -o "${install_script}"
  UV_INSTALL_DIR=/usr/local/bin sh "${install_script}"
  rm -f "${install_script}"
  trap - EXIT
fi

log "installing Python 3.12 with uv"
export UV_PYTHON_INSTALL_DIR
uv python install 3.12
python_path=$(uv python find 3.12)
ln -sfn "${python_path}" /usr/local/bin/python3.12

log "creating protected configuration and persistent state directories"
install -d -o root -g root -m 0750 "${CONFIG_DIR}" "${STATE_DIR}" "${STATE_DIR}/postgres" "${STATE_DIR}/redis"
install -o root -g root -m 0600 "${REPO_DIR}/infra/vast/runtime.env.example" "${RUNTIME_TEMPLATE}"

if [[ ! -e "${RUNTIME_ENV}" ]]; then
  postgres_password=$(openssl rand -hex 32)
  redis_password=$(openssl rand -hex 32)
  {
    printf 'TRADING_ENABLED=false\n'
    printf 'ALPACA_API_KEY_ID=\n'
    printf 'ALPACA_API_SECRET_KEY=\n'
    printf 'ALPACA_PAPER_BASE_URL=https://paper-api.alpaca.markets\n'
    printf 'ALPACA_DATA_FEED=iex\n'
    printf 'OPTIONS_API_KEY=\n'
    printf 'OPTIONS_API_BASE_URL=\n'
    printf 'POSTGRES_DB=institutional_signal\n'
    printf 'POSTGRES_USER=signal_engine\n'
    printf 'POSTGRES_PASSWORD=%s\n' "${postgres_password}"
    printf 'REDIS_PASSWORD=%s\n' "${redis_password}"
    printf 'DATABASE_URL=postgresql://signal_engine:%s@127.0.0.1:5432/institutional_signal\n' "${postgres_password}"
    printf 'REDIS_URL=redis://:%s@127.0.0.1:6379/0\n' "${redis_password}"
  } >"${RUNTIME_ENV}"
fi
chown root:root "${RUNTIME_ENV}"
chmod 0600 "${RUNTIME_ENV}"

if grep -q '^TRADING_ENABLED=' "${RUNTIME_ENV}"; then
  sed -i 's/^TRADING_ENABLED=.*/TRADING_ENABLED=false/' "${RUNTIME_ENV}"
else
  printf 'TRADING_ENABLED=false\n' >>"${RUNTIME_ENV}"
fi

log "hardening SSH password authentication while retaining key access"
install -d -m 0755 /etc/ssh/sshd_config.d
cat >/etc/ssh/sshd_config.d/60-institutional-signal-engine.conf <<'EOF'
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitRootLogin prohibit-password
EOF
sshd -t
systemctl reload ssh

log "installing and starting restart-safe dependency services"
install -o root -g root -m 0644 \
  "${REPO_DIR}/infra/vast/systemd/institutional-signal-dependencies.service" \
  /etc/systemd/system/institutional-signal-dependencies.service
systemctl daemon-reload
systemctl enable --now docker
systemctl enable --now unattended-upgrades
systemctl enable --now institutional-signal-dependencies.service

log "bootstrap complete; trading remains disabled"
