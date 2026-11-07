#!/usr/bin/env bash
# Runs the boundary benchmark on a fresh Ubuntu 24.04 machine without Docker,
# such as a cloud pod: installs Postgres 18 (with the compose file's role and
# database), Go and Rust, then runs make bench-boundary and saves the report
# with the machine's details. Run it as root from the repository root; it is
# safe to run again. Arguments are passed to the benchmark, e.g. -rows 10000000.
# It needs a copy of openetl-connectors at CONNECTORS_SRC (default
# ../openetl-connectors), which make connectors builds against this checkout.
# OPENETL_COMMIT and OPENETL_CONNECTORS_COMMIT name the commits in the report
# when there is no .git, as in a copy made with git archive.
# With ICEBERG=1, it also runs SeaweedFS and Iceberg's REST catalog, without
# Docker, for -lanes; destination-iceberg's config is then in $ICEBERG_CONFIG.
# The catalog is only published as an image, so copy its jar to
# /opt/iceberg-rest.jar first, from /usr/lib/iceberg-rest/iceberg-rest-adapter.jar
# in apache/iceberg-rest-fixture:1.10.1.
set -euo pipefail

connectors=${CONNECTORS_SRC:-../openetl-connectors}
[ -f "$connectors/go.mod" ] || { echo "no openetl-connectors at $connectors; copy it there or set CONNECTORS_SRC" >&2; exit 1; }

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

if [ -n "${ICEBERG:-}" ]; then
  apt-get install -yq --no-install-recommends default-jre-headless
  [ -x /usr/local/bin/weed ] || curl -fsSL https://github.com/seaweedfs/seaweedfs/releases/download/4.51/linux_amd64.tar.gz | tar -C /usr/local/bin -xz weed
  [ -f /opt/iceberg-rest.jar ] || { echo "no /opt/iceberg-rest.jar: copy it from apache/iceberg-rest-fixture:1.10.1" >&2; exit 1; }
  mkdir -p /var/lib/weed
  pgrep -x weed >/dev/null || WEED_MASTER_VOLUME_GROWTH_COPY_1=1 nohup weed server -dir=/var/lib/weed -volume.max=0 -s3 -s3.port=8333 -master.volumeSizeLimitMB=1024 >/var/log/weed.log 2>&1 &
  # The image's environment: a catalog in a file, as each pooled connection
  # to SQLite's in-memory one has a database of its own.
  pgrep -f iceberg-rest.jar >/dev/null || CATALOG_CATALOG__IMPL=org.apache.iceberg.jdbc.JdbcCatalog CATALOG_URI=jdbc:sqlite:/var/lib/iceberg-catalog.db \
    CATALOG_JDBC_USER=user CATALOG_JDBC_PASSWORD=password CATALOG_JDBC_STRICT__MODE=true \
    CATALOG_WAREHOUSE=s3://iceberg/ CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO \
    CATALOG_S3_ENDPOINT=http://127.0.0.1:8333 CATALOG_S3_PATH__STYLE__ACCESS=true AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=openetl AWS_SECRET_ACCESS_KEY=openetl \
    nohup java -jar /opt/iceberg-rest.jar >/var/log/iceberg-rest.log 2>&1 &
  # 409 once the bucket exists, as on a second run.
  until curl -s -o /dev/null -w '%{http_code}' -X PUT http://127.0.0.1:8333/iceberg | grep -q '^200$\|^409$'; do sleep 1; done
  until curl -fs http://127.0.0.1:8181/v1/config >/dev/null 2>&1; do sleep 1; done
  export ICEBERG_CONFIG='{"uri": "http://127.0.0.1:8181", "namespace": "bench", "s3Endpoint": "http://127.0.0.1:8333", "region": "us-east-1", "accessKeyId": "openetl", "secretAccessKey": "openetl"}'
fi

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

# The first cgroup v2 file, else the first v1 file, that exists.
cgroup() { for f; do [ -r "/sys/fs/cgroup/$f" ] && { echo "$f: $(cat "/sys/fs/cgroup/$f")"; return; }; done; echo n/a; }
report="bench-boundary-$(date -u +%Y%m%dT%H%M%SZ).txt"
{
  echo "# $(date -u +%FT%TZ) ${OPENETL_COMMIT:-$(git rev-parse --short HEAD 2>/dev/null || echo unknown)}, connectors ${OPENETL_CONNECTORS_COMMIT:-$(git -C "$connectors" rev-parse --short HEAD 2>/dev/null || echo unknown)}"
  echo "kernel: $(uname -sr)"
  echo "nproc: $(nproc)"
  echo "cgroup cpu: $(cgroup cpu.max cpu/cpu.cfs_quota_us)"
  echo "cgroup cpuset: $(cgroup cpuset.cpus.effective cpuset/cpuset.cpus)"
  echo "memory: $(awk '/MemTotal/ {print $2 " kB"}' /proc/meminfo)"
  echo "cgroup memory: $(cgroup memory.max memory/memory.limit_in_bytes)"
  # A container's network namespace may hide wmem_max, so also ask for the edges' 1 MiB.
  echo "net.core.wmem_max: $(cat /proc/sys/net/core/wmem_max 2>/dev/null || echo hidden)"
  echo "SO_SNDBUF granted for 1 MiB: $(perl -MSocket -e 'socketpair(my $s, my $t, AF_UNIX, SOCK_STREAM, 0) or die $!; setsockopt($s, SOL_SOCKET, SO_SNDBUF, 1 << 20) or die $!; print unpack("i", getsockopt($s, SOL_SOCKET, SO_SNDBUF))' 2>&1)"
  lscpu | grep -E '^(Model name|Thread|Core|Socket|CPU\(s\)|L3)'
  su postgres -c "psql -qtA -c 'SELECT version()'"
  echo
} | tee "$report"
make bench-boundary CONNECTORS_SRC="$connectors" BENCH_FLAGS="$*" 2>&1 | tee -a "$report"
echo "report: $report"
