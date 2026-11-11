---
title: Install the CLI
description: Install the openetl command-line tool on macOS or Linux, check the download, connect it to an installation, and upgrade or remove it.
---

`openetl` is OpenETL's command-line tool. You use it to apply pipelines, start syncs, and manage secrets, connectors and users. The CLI calls the API of an OpenETL installation. That installation can run on your computer or on a server in another place.

The CLI runs on macOS and Linux, on amd64 (x86-64) and arm64 processors. There is no build for Windows.

## Install with one command

Run this command in a terminal:

```sh
curl -fsSL https://raw.githubusercontent.com/openetl-io/openetl/main/install.sh | sh
```

```text
Downloading openetl 0.1.0-alpha for darwin/arm64 from https://github.com/openetl-io/openetl/releases/download/v0.1.0-alpha
Installed openetl 0.1.0-alpha at /Users/alex/.local/bin/openetl
```

The script does these steps:

1. It finds your operating system and processor.
2. It downloads the CLI and `checksums.txt` from the OpenETL 0.1.0-alpha release on GitHub.
3. It compares the SHA-256 of the download with `checksums.txt`. If they are different, it stops and installs nothing.
4. It puts `openetl` in `~/.local/bin`, and replaces an earlier `openetl` there.

If `~/.local/bin` is not on your `PATH`, the script shows a line to add to your shell's profile. Add the line, then open a new terminal.

Check the installation:

```sh
openetl version
```

```text
0.1.0-alpha
```

To read the script before you run it, download it first:

```sh
curl -fsSLO https://raw.githubusercontent.com/openetl-io/openetl/main/install.sh
less install.sh
sh install.sh
```

### Script settings

The script reads these environment variables:

| Variable | Default | What it sets |
|---|---|---|
| `OPENETL_VERSION` | `0.1.0-alpha` | The release to install, such as `0.1.0-alpha` or `v0.1.0-alpha` |
| `OPENETL_INSTALL_DIR` | `$HOME/.local/bin` | The directory for `openetl`. The script creates it if it does not exist. |
| `OPENETL_DOWNLOAD_URL` | `https://github.com/openetl-io/openetl/releases/download` | Where to download from. A mirror must have the same paths: `vVERSION/FILE`. |

Put a variable before `sh`. For example, this command installs into `~/bin`:

```sh
curl -fsSL https://raw.githubusercontent.com/openetl-io/openetl/main/install.sh | OPENETL_INSTALL_DIR="$HOME/bin" sh
```

## Install by hand

Use this procedure if you cannot run a script from the internet. Replace `OS` with `darwin` or `linux`, and `ARCH` with `amd64` or `arm64`.

1. Download the archive and `checksums.txt` from the release:

   ```sh
   curl -fsSLO https://github.com/openetl-io/openetl/releases/download/v0.1.0-alpha/openetl-cli_0.1.0-alpha_OS_ARCH.tar.gz
   curl -fsSLO https://github.com/openetl-io/openetl/releases/download/v0.1.0-alpha/checksums.txt
   ```

2. Check the archive's SHA-256:

   ```sh
   grep ' openetl-cli_0.1.0-alpha_OS_ARCH.tar.gz$' checksums.txt | shasum -a 256 -c
   ```

   ```text
   openetl-cli_0.1.0-alpha_OS_ARCH.tar.gz: OK
   ```

   If the output does not show `OK`, delete the archive and do not use it. On Linux, `sha256sum -c` does the same check.

3. Extract the archive, and move `openetl` to a directory on your `PATH`:

   ```sh
   tar -xzf openetl-cli_0.1.0-alpha_OS_ARCH.tar.gz
   mv openetl-cli_0.1.0-alpha_OS_ARCH/openetl ~/.local/bin/
   ```

The archive also holds `LICENSE`, `NOTICE` and `THIRD-PARTY-NOTICES.txt`.

### Check the signature

The release workflow signs `checksums.txt` with [Sigstore](https://www.sigstore.dev) keyless signing. The signature shows that the file comes from this repository's release workflow. To check it, install [cosign](https://docs.sigstore.dev/cosign/system_config/installation/) 3 or later. Then, in the directory that holds `checksums.txt`, run:

```sh
curl -fsSLO https://github.com/openetl-io/openetl/releases/download/v0.1.0-alpha/checksums.txt.sigstore.json
cosign verify-blob checksums.txt --bundle checksums.txt.sigstore.json \
  --certificate-identity-regexp '^https://github\.com/openetl-io/openetl/\.github/workflows/release\.yml@' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

```text
Verified OK
```

A file whose signature you checked gives the correct SHA-256 for each archive in the release.

## Connect to an installation

By default, the CLI calls the API at `http://localhost:8080`. It reads its API key from `.openetl/admin.key` in the current directory. To use another installation, set its URL and an API key:

```sh
export OPENETL_API_URL=https://openetl.example.com
export OPENETL_API_KEY="$(cat ~/openetl.key)"
openetl whoami
```

`openetl whoami` shows the principal that the key belongs to, and its roles. If you do not have an API key, ask an administrator of the installation. [Manage access and credentials](../self-host/access.md) shows how an administrator makes one.

Each command also takes `--api-url` and `--api-key-file`. [The openetl CLI](../reference/index.md#the-openetl-cli) lists the commands and these settings.

## Upgrade

Use a CLI of the same version as the installation that it calls. The pipeline format and the API can change between alpha releases.

To change the version, run the install command again with `OPENETL_VERSION`:

```sh
curl -fsSL https://raw.githubusercontent.com/openetl-io/openetl/main/install.sh | OPENETL_VERSION=0.1.0-alpha sh
```

The [releases page](https://github.com/openetl-io/openetl/releases) lists each version and its changes.

## Remove

Delete the program:

```sh
rm ~/.local/bin/openetl
```

The CLI does not keep settings. `openetl connector` commands that run a package can download connector executables to a cache: `~/Library/Caches/openetl` on macOS, or `~/.cache/openetl` on Linux. Delete that directory too if it exists.
