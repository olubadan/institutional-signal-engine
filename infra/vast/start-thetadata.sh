#!/usr/bin/env bash
set -Eeuo pipefail

readonly runtime_env=/etc/institutional-signal-engine/runtime.env
readonly terminal_dir=/opt/thetadata
readonly state_dir=/var/lib/institutional-signal-engine/thetadata

[[ -r ${runtime_env} ]] || { printf 'ThetaData runtime environment is unavailable\n' >&2; exit 1; }
theta_api_key=
while IFS='=' read -r key value; do
  if [[ ${key} == THETADATA_API_KEY ]]; then
    theta_api_key=${value%$'\r'}
    break
  fi
done <"${runtime_env}"
[[ -n ${theta_api_key} ]] || { printf 'THETADATA_API_KEY is not configured\n' >&2; exit 1; }
export THETADATA_API_KEY="${theta_api_key}"

cd "${state_dir}"
exec /usr/bin/docker run --rm \
  --name institutional-signal-thetadata \
  --network host \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=128m \
  --env THETADATA_API_KEY \
  --volume "${terminal_dir}:/opt/thetadata:ro" \
  --volume "${state_dir}:/var/lib/thetadata:rw" \
  --workdir /var/lib/thetadata \
  eclipse-temurin:21-jre \
  java -jar /opt/thetadata/ThetaTerminalv3.jar
