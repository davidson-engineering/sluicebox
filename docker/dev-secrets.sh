#!/bin/sh
# Generates random throwaway credentials for the local docker compose servers into
# docker/secrets/ (git-ignored). The compose "secrets" service runs it on every
# `docker compose up`; existing files are kept, so the servers and the tests always agree.
set -eu

dir="${1:-/secrets}"
umask 022

random() {
    od -An -N32 -tx1 /dev/urandom | tr -d ' \n'
}

[ -s "$dir/influxdb2-token" ] || random >"$dir/influxdb2-token"
[ -s "$dir/influxdb2-password" ] || random >"$dir/influxdb2-password"
if [ ! -s "$dir/influxdb3-token" ] || [ ! -s "$dir/influxdb3-admin-token.json" ]; then
    token="apiv3_$(random)"
    printf '{"token": "%s", "name": "_admin"}\n' "$token" >"$dir/influxdb3-admin-token.json"
    printf '%s' "$token" >"$dir/influxdb3-token"
fi
