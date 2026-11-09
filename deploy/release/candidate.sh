#!/usr/bin/env bash
# A release candidate's files for the test suites, and discarding a candidate.
#
#   candidate.sh fetch VERSION DIR   download this platform's files into DIR and
#                                    check them against the candidate's checksums
#   candidate.sh unpack DIR          unpack what fetch wrote, for
#                                    make e2e CANDIDATE=DIR and make chaos CANDIDATE=DIR
#   candidate.sh discard VERSION     delete the candidate's private packages and
#                                    its draft release
#
# It needs gh signed in with write access to the repository, as only that shows
# a draft; discard needs the delete:packages scope too, or a workflow's token.
set -euo pipefail

repo=${GITHUB_REPOSITORY:-openetl-io/openetl}
org=${repo%%/*}

die() {
  echo "candidate: $*" >&2
  exit 1
}

# Whether vVERSION is a draft: true, false, or none when there's no release.
is_draft() {
  gh release view "v$1" -R "$repo" --json isDraft --jq .isDraft 2>/dev/null || echo none
}

fetch() {
  local version=$1 dir=$2 os arch
  os=$(uname -s | tr '[:upper:]' '[:lower:]')
  case $(uname -m) in
    x86_64 | amd64) arch=amd64 ;;
    aarch64 | arm64) arch=arm64 ;;
    *) die "no candidate is built for $(uname -m)" ;;
  esac
  [ "$(is_draft "$version")" = true ] || die "v$version has no draft release"
  mkdir -p "$dir"
  gh release download "v$version" -R "$repo" -D "$dir" --clobber \
    -p "openetl_${version}_${os}_${arch}.tar.gz" -p "connectors_${version}_${os}_${arch}.tar.gz" \
    -p 'openetl-*.jar' -p checksums.txt -p candidate.json
  (
    cd "$dir"
    if command -v sha256sum >/dev/null; then
      sha256sum --ignore-missing -c checksums.txt
    else
      shasum -a 256 --ignore-missing -c checksums.txt
    fi
  )
}

unpack() {
  local dir=$1
  mkdir -p "$dir/connectors" "$dir/lib"
  tar -xzf "$dir"/openetl_*.tar.gz -C "$dir" --strip-components=1
  tar -xzf "$dir"/connectors_*.tar.gz -C "$dir/connectors"
  mv "$dir"/openetl-spark-*.jar "$dir"/openetl-flink-*.jar "$dir/lib/"
}

discard() {
  local version=$1 draft package
  draft=$(is_draft "$version")
  [ "$draft" != false ] || die "v$version is published, so it isn't a candidate"
  # Packages first: if deleting one fails, the draft is still there to retry from.
  while read -r package; do
    [ -n "$package" ] || continue
    echo "deleting package $package"
    gh api -X DELETE "/orgs/$org/packages/container/${package//\//%2F}"
  done < <(gh api --paginate "/orgs/$org/packages?package_type=container" --jq '.[].name' | grep -F "candidates/$version/" || true)
  if [ "$draft" = true ]; then
    gh release delete "v$version" -R "$repo" --yes
  fi
}

case ${1:-} in
  fetch) [ $# = 3 ] || die "fetch takes VERSION DIR"; fetch "$2" "$3" ;;
  unpack) [ $# = 2 ] || die "unpack takes DIR"; unpack "$2" ;;
  discard) [ $# = 2 ] || die "discard takes VERSION"; discard "$2" ;;
  *) die "usage: candidate.sh fetch VERSION DIR | unpack DIR | discard VERSION" ;;
esac
