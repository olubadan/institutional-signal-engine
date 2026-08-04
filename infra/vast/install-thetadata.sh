#!/usr/bin/env bash
set -Eeuo pipefail

readonly terminal_dir=/opt/thetadata
readonly state_dir=/var/lib/institutional-signal-engine/thetadata
readonly jar_url=https://downloads.thetadata.us/ThetaTerminalv3.jar

[[ ${EUID} -eq 0 ]] || { printf 'run as root\n' >&2; exit 1; }
apt-get update -qq
apt-get install -y --no-install-recommends openjdk-21-jre-headless
id -u thetadata >/dev/null 2>&1 || useradd --system --home-dir "${state_dir}" --shell /usr/sbin/nologin thetadata
install -d -o root -g root -m 0755 "${terminal_dir}"
install -d -o thetadata -g thetadata -m 0750 "${state_dir}"
if [[ ! -s ${terminal_dir}/ThetaTerminalv3.jar ]]; then
  temporary=$(mktemp "${terminal_dir}/ThetaTerminalv3.jar.XXXXXX")
  trap 'rm -f "${temporary}"' EXIT
  curl -fsSL "${jar_url}" -o "${temporary}"
  unzip -tqq "${temporary}"
  chown root:root "${temporary}"
  chmod 0644 "${temporary}"
  mv "${temporary}" "${terminal_dir}/ThetaTerminalv3.jar"
  trap - EXIT
fi
install -o root -g root -m 0755 infra/vast/start-thetadata.sh /usr/local/sbin/start-institutional-signal-thetadata
install -o root -g root -m 0644 infra/vast/systemd/institutional-signal-thetadata.service /etc/systemd/system/institutional-signal-thetadata.service
systemctl daemon-reload
systemctl enable --now institutional-signal-thetadata.service
