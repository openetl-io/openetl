---
title: Console tasks with the CLI
description: Find command-line equivalents for common console actions.
---

Use this page if you work in a terminal. First, [install the CLI and connect it to your installation](../get-started/install.md).

## With the CLI

The `openetl` command shows most of what the console does, and does what the console cannot. Replace `PIPELINE` with the pipeline's name:

```sh
openetl syncs PIPELINE                   # its syncs, newest first
openetl logs PIPELINE                    # its latest sync's log; -f follows a running one
openetl versions PIPELINE                # its versions, newest first
openetl plan -f FILE                     # SQL: the plan of the pipelines in FILE
openetl sync PIPELINE                    # Sync now
openetl pause PIPELINE                   # Pause
openetl resume PIPELINE                  # Resume
openetl resume --stream STREAM PIPELINE  # Resume stream
openetl overrides PIPELINE               # Not as the spec says
openetl overrides remove PIPELINE FIELD  # Use the spec's
openetl incident list                    # open incidents
openetl incident get PIPELINE INCIDENT   # one incident: what happened and what to do
openetl incident resolve PIPELINE INCIDENT
openetl holds PIPELINE                   # what it holds back
openetl holds release PIPELINE STREAM COLUMN
openetl holds discard PIPELINE STREAM COLUMN
openetl quarantine PIPELINE              # the files of rows it quarantined, and why
openetl quarantine show PIPELINE PATH    # Show records
openetl preflight PIPELINE               # Preflight
openetl check source NAME                # Check, on a source's page
openetl check destination NAME           # Check, on a destination's page
```

`openetl check` and `openetl preflight` print the same items as the console. [Pipeline CLI commands](../reference/pipeline-cli.md#text-output) explains their output.

`openetl principal password-link NAME` makes the link a person sets their password with, and `openetl principal sign-out NAME` signs them out everywhere. `openetl COMMAND -h` lists a command's flags.
