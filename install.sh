#!/bin/sh
# Installs the openetl CLI on macOS or Linux, from a release of
# github.com/openetl-io/openetl, once its archive matches the SHA-256 in the
# release's checksums.txt:
#
#   curl -fsSL https://raw.githubusercontent.com/openetl-io/openetl/main/install.sh | sh
#
# It reads these from the environment:
#
#   OPENETL_VERSION       the release to install, such as 0.1.0-alpha
#                         (default: the release the docs beside this file
#                         describe, which test/docs checks)
#   OPENETL_INSTALL_DIR   where openetl goes (default: $HOME/.local/bin)
#   OPENETL_DOWNLOAD_URL  where the releases' files are, by tag, for a mirror
#                         (default: GitHub's)
set -eu

version=${OPENETL_VERSION:-0.1.0-alpha}
version=${version#v}
dir=${OPENETL_INSTALL_DIR:-$HOME/.local/bin}
base=${OPENETL_DOWNLOAD_URL:-https://github.com/openetl-io/openetl/releases/download}
base=${base%/}

fail() {
	echo "install.sh: $*" >&2
	exit 1
}

case $(uname -s) in
Darwin) os=darwin ;;
Linux) os=linux ;;
*) fail "there is no openetl CLI for $(uname -s): only macOS and Linux" ;;
esac
case $(uname -m) in
x86_64 | amd64) arch=amd64 ;;
arm64 | aarch64) arch=arm64 ;;
*) fail "there is no openetl CLI for $(uname -m): only amd64 and arm64" ;;
esac

command -v curl >/dev/null 2>&1 || fail "curl is not installed"
command -v tar >/dev/null 2>&1 || fail "tar is not installed"
if command -v sha256sum >/dev/null 2>&1; then
	sha256() { sha256sum "$1" | cut -d ' ' -f 1; }
elif command -v shasum >/dev/null 2>&1; then
	sha256() { shasum -a 256 "$1" | cut -d ' ' -f 1; }
else
	fail "neither sha256sum nor shasum is installed"
fi

name=openetl-cli_${version}_${os}_${arch}
url=$base/v$version
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

echo "Downloading openetl $version for $os/$arch from $url"
curl -fsSL -o "$tmp/$name.tar.gz" "$url/$name.tar.gz" || fail "cannot download $url/$name.tar.gz"
curl -fsSL -o "$tmp/checksums.txt" "$url/checksums.txt" || fail "cannot download $url/checksums.txt"

want=$(awk -v f="$name.tar.gz" '$2 == f { print $1 }' "$tmp/checksums.txt")
[ -n "$want" ] || fail "checksums.txt has no line for $name.tar.gz"
got=$(sha256 "$tmp/$name.tar.gz")
[ "$got" = "$want" ] || fail "$name.tar.gz has SHA-256 $got, but checksums.txt gives $want"

tar -xzf "$tmp/$name.tar.gz" -C "$tmp"
[ -f "$tmp/$name/openetl" ] || fail "$name.tar.gz has no $name/openetl"
mkdir -p "$dir"
cp "$tmp/$name/openetl" "$tmp/openetl.new"
chmod 755 "$tmp/openetl.new"
mv -f "$tmp/openetl.new" "$dir/openetl"
echo "Installed openetl $("$dir/openetl" version) at $dir/openetl"

case ":$PATH:" in
*":$dir:"*) ;;
*)
	echo
	echo "$dir is not on your PATH. Add this line to your shell's profile, such as ~/.zshrc or ~/.bashrc:"
	echo
	echo "  export PATH=\"$dir:\$PATH\""
	;;
esac
