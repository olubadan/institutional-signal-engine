#!/usr/bin/env bash
set -Eeuo pipefail

readonly runtime_env=/etc/institutional-signal-engine/runtime.env
readonly terminal_dir=/opt/thetadata
readonly state_dir=/var/lib/institutional-signal-engine/thetadata

[[ -r ${runtime_env} ]] || { printf 'ThetaData runtime environment is unavailable\n' >&2; exit 1; }
set -a
# shellcheck source=/dev/null
source "${runtime_env}"
set +a
[[ -n ${THETADATA_API_KEY:-} ]] || { printf 'THETADATA_API_KEY is not configured\n' >&2; exit 1; }

cd "${state_dir}"
exec env -i \
  HOME="${state_dir}" \
  PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
  THETADATA_API_KEY="${THETADATA_API_KEY}" \
  /usr/bin/java -jar "${terminal_dir}/ThetaTerminalv3.jar"
