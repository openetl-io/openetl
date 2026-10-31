#!/usr/bin/env bash
# Runs the boundary benchmark on a fresh Ubuntu 24.04 machine without Docker,
# such as a cloud pod: installs Postgres 18 (with the compose file's role and
# database), Go and Rust, then runs make bench-boundary and saves the report
# with the machine's details. Run it as root from the repository root; it is
# safe to run again. Arguments are passed to the benchmark, e.g. -rows 10000000.
# OPENETL_COMMIT names the commit in the report when there is no .git, as in a
# copy made with git archive.
set -euo pipefail

export DEBIAN_FRONTEND=noninteractive
export PATH="/usr/local/go/bin:$HOME/.cargo/bin:$PATH"

if ! command -v pg_ctlcluster >/dev/null || ! [ -d /usr/lib/postgresql/18 ]; then
  apt-get update -q
  apt-get install -yq --no-install-recommends ca-certificates curl gnupg build-essential make lsb-release
  install -d /usr/share/postgresql-common/pgdg
  curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc https://www.postgresql.org/media/keys/ACCC4CF8.asc
  echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
    >/etc/apt/sources.list.d/pgdg.list
  apt-get update -q
  apt-get install -yq --no-install-recommends postgresql-18
fi

# Containers have no init, so the package can't start the cluster itself.
pg_ctlcluster 18 main status >/dev/null || pg_ctlcluster 18 main start
su postgres -c "psql -qtA -c \"SELECT 1 FROM pg_roles WHERE rolname = 'openetl'\"" | grep -q 1 ||
  su postgres -c "psql -q -c \"CREATE ROLE openetl LOGIN SUPERUSER PASSWORD 'openetl'\""
su postgres -c "psql -qtA -c \"SELECT 1 FROM pg_database WHERE datname = 'openetl'\"" | grep -q 1 ||
  su postgres -c "createdb -O openetl openetl"

go_version=$(sed -n 's/^toolchain go//p' go.mod)
if [ "$(go env GOVERSION 2>/dev/null)" != "go$go_version" ]; then
  case $(uname -m) in
    x86_64) goarch=amd64 ;;
    aarch64) goarch=arm64 ;;
    *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
  esac
  rm -rf /usr/local/go
  curl -fsSL "https://go.dev/dl/go$go_version.linux-$goarch.tar.gz" | tar -C /usr/local -xz
fi
# rustup installs the toolchain rust-toolchain.toml pins on first use.
command -v rustup >/dev/null || curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain none

report="bench-boundary-$(date -u +%Y%m%dT%H%M%SZ).txt"
{
  echo "# $(date -u +%FT%TZ) ${OPENETL_COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}"
  echo "kernel: $(uname -sr)"
  echo "nproc: $(nproc)"
  echo "cgroup cpu.max: $(cat /sys/fs/cgroup/cpu.max 2>/dev/null || echo n/a)"
  echo "memory: $(awk '/MemTotal/ {print $2 " kB"}' /proc/meminfo)"
  echo "cgroup memory.max: $(cat /sys/fs/cgroup/memory.max 2>/dev/null || echo n/a)"
  echo "net.core.wmem_max: $(cat /proc/sys/net/core/wmem_max)"
  lscpu | grep -E '^(Model name|Thread|Core|Socket|CPU\(s\)|L3)'
  su postgres -c "psql -qtA -c 'SELECT version()'"
  echo
} | tee "$report"
make bench-boundary BENCH_FLAGS="$*" 2>&1 | tee -a "$report"
echo "report: $report"
