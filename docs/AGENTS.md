# Writing the docs

For people and coding agents. [The repository's AGENTS.md](../AGENTS.md) applies here too.

This folder is OpenETL's documentation. The docs site, in [docs-web](https://github.com/openetl-io/docs-web), is built with Fumadocs from this folder at `main` and at each release tag, so a release's docs are this folder as it was at its tag. The same files are read on GitHub, so every page has to read well in both places.

Pages about one connector live with that connector: the official ones in [openetl-connectors](https://github.com/openetl-io/openetl-connectors), whose packages the site lists under Connectors. This folder covers OpenETL itself, and the connectors built into it (`faker`, `stdout` and `parquet`) in the reference.

External connectors may be official, community maintained or private, with versions and release cadences independent of OpenETL. Explain the engine in terms of connector roles and capabilities; keep lists of systems, setup requirements and connector-specific limits with the connectors. A worked example may name a connector, but must identify it as an example and link to its own documentation.

## Who each section is for

Write each page for its section's reader. Before writing, ask what they know, what they came to do, and what they'll do next.

| Folder | Reader | What they know | How to write for them |
|---|---|---|---|
| `get-started` | Anyone new to OpenETL | Nothing about it yet | Plain words. No internals, no YAML beyond the quickstart's. |
| `use` | People who depend on the data: analysts, product managers, business people, engineers | Their own data and tools; not YAML, shells or Kubernetes | Start from the console, and name screens, tabs and buttons exactly as it labels them. Explain every term on first use or link to [Concepts](./get-started/concepts.md). A CLI equivalent goes last, under its own heading. |
| `pipelines` | Data and analytics engineers who write pipelines | SQL, YAML, a shell, git | Task first. Show the YAML, then what it does. Link to the reference rather than listing every field. |
| `integrate` | People connecting other systems: dbt, Spark, Flink, the API, webhooks, OpenTelemetry, query engines | The other system | Say what OpenETL sends or expects, and show a working setup on each side. |
| `self-host` | People who install, run and upgrade OpenETL | Linux, containers, Postgres, object storage; Kubernetes where a page says so | Exact commands, the settings each step needs, how to check it worked, and how to undo it. |
| `build-connectors` | Connector developers | Go, or another language and gRPC | The contract, then a connector that works, then how to test, package and publish it. |
| `reference` | Anyone looking something up | What they're looking for | Complete and exact: every field, flag, command or event, with its type, required or optional status, default and meaning. State conditional requirements, including mutually exclusive choices, explicitly. No tutorials. |
| `internals` | Contributors, and readers who want to know how it works | Distributed systems | How and why, at the level of the code. May name packages and protocols. |

Keep to one kind of page at a time: a tutorial walks through something that works end to end, a how-to does one task, a reference page lists, an explanation explains. A how-to that needs every field of something links to its reference page.

## What the docs say

- **What the code at this commit does, and nothing else.** No roadmaps, dates, "coming soon" or features a release doesn't have, and nothing about products or services outside these repositories. State a limit plainly, without a reason or a promise.
- **A change and its docs land together.** A pull request that changes what someone using, running or integrating OpenETL sees (a command or flag, a field of a spec, a setting, an event, a metric, an API, a screen or a label in the console, a default, an error people act on) updates the pages that describe it.
- **Run what you show.** Every command and every YAML snippet has been run against the code at that commit, and output is copied from a real run, trimmed only where the page says so. A whole pipeline belongs in [`examples/`](../examples), where `TestExamples` parses it, and the page links to it or quotes it.
- **One source of truth.** Where the code holds the truth, such as a proto's comments, `openetld`'s settings, the CLI's usage or a JSON Schema, the reference follows it word for word where it can. Don't keep a second description that drifts.

## Format

- **Markdown**, CommonMark with GitHub's extensions (tables, task lists, strikethrough), in `.md` files. No MDX or JSX, and no raw HTML: the site drops HTML in `.md` files, comments included.
- **Frontmatter** on every page: `title`, short and in sentence case, and `description`, one sentence that says what the page is for; search results and link previews show it.

  ```yaml
  ---
  title: Install on Kubernetes
  description: Run OpenETL's server, orchestrators and runners on a Kubernetes cluster with plain manifests.
  ---
  ```

- **No `# H1`.** The site renders the title as the heading. The body starts with a paragraph, and its headings start at `##`, in sentence case, without skipping levels. Headings are link targets, so don't reword one without fixing the links to it.
- **Folders** each have a `meta.json` that lists their pages in order, by file name without `.md`, and gives the folder's `title`. A section's `meta.json` also has a `description` and an `icon` (a [Lucide](https://lucide.dev/icons) name), which the site shows beside the section in its sidebar. No folder has `root`: the site keeps every section in the sidebar at once. A page that isn't listed doesn't appear on the site.
- **File names** are lowercase words joined by hyphens, and name the page's subject or task: `kubernetes.md`, `schema-changes.md`. A folder's landing page is `index.md`.
- **Links** between pages are relative and start with `./` or `../`, with the `.md`: `[Concepts](./concepts.md#delivery-guarantees)`. The site resolves only those. Links to files outside `docs/`, such as an example or a manifest, are relative too, and the site points them at GitHub at the same commit. Links to other repositories and sites are full URLs.
- **Callouts** use GitHub's alert syntax, which GitHub renders and the site turns into callouts. Use them sparingly: one a page is plenty.

  ```markdown
  > [!WARNING]
  > This deletes every volume the compose stack made.
  ```

  The kinds are `NOTE`, `TIP`, `IMPORTANT`, `WARNING` and `CAUTION`.
- **Code blocks** always name their language: `sh`, `yaml`, `json`, `go`, `sql`, and `text` for output. Commands have no prompt, and output goes in a `text` block of its own after them. Write placeholders in capitals, as the CLI's usage does: `openetl connector apply oci://REGISTRY/REPOSITORY:VERSION`.
- **Diagrams** are `mermaid` code blocks, which GitHub and the site both draw. Keep them small.
- **Images** go in `docs/images/`, as SVG or PNG, with alt text that says what the image shows. Show the console in a screenshot only where words can't carry it, since every change to the UI makes one stale: take it from `pnpm dev:mock` in `web/`, 1280 pixels wide.

## Style

- Write to the reader as "you", in the present tense and the active voice, in short sentences and plain words. American spelling.
- Say what something does before how to configure it, and what to do before why.
- Call things by one name everywhere, the one [Concepts](./get-started/concepts.md) and the console use. A new term gets a line there.
- In prose the product is OpenETL. The programs, commands and files are in code font: `openetld`, `openetl apply`, `openetl-engine`, `.openetl/admin.key`.
- Labels in the console are in bold, exactly as the console shows them: **Sync now**, **Pause**.
- Units are spelled out or standard: 15 minutes, 8 vCPUs, 16 GiB. Durations in YAML are Go durations (`90s`, `1h`).
- No filler or sales words: easy, simple, just, simply, powerful, seamless.

### Simplified Technical English

Use the writing principles in [ASD-STE100](https://www.asd-ste100.org/assets/files/ASD-STE100_ISSUE9.pdf) for reader-facing prose. Keep software terms, identifiers and console labels exact. These guidelines adapt the standard to OpenETL; they do not establish full compliance with its dictionary.

- Give one instruction per sentence. Start instructions with a command verb: "Open", "Select", "Run" or "Check".
- Keep instruction sentences to 20 words or fewer and descriptive sentences to 25 words or fewer. Split longer sentences at a change of action or topic. Do not remove articles or necessary details to meet a word limit.
- State a condition before the action it controls: "If the pipeline is paused, resume it."
- Name the component that acts. Use "The runner reads the token" instead of "The token is read".
- Use one term for one meaning. Use "write" or "commit" when describing delivery; avoid figurative terms such as "land".
- Write complete words instead of contractions: "does not", "cannot" and "it is".
- Keep each paragraph about one subject, with no more than six sentences. Use a list for separate conditions or choices.
- Put prerequisites before a procedure. State the expected result after the action and explain how to check it.
- Put a warning before the command that can delete data or interrupt service. State the consequence explicitly. Notes give information; they do not contain required steps.
- Keep commands, configuration fields, protocol values and quoted output unchanged during a prose edit. Change them only after checking their source and running the revised example.

## Checking a change

`go test ./test/docs` checks the rules a machine can: every page's frontmatter, no `# H1`, no raw HTML, alt text on images, links that resolve with anchors that exist, a `meta.json` in every folder that lists each of its pages, a row on the resource spec pages for every field `internal/spec` reads, example documents there that parse, and generated pages that match their sources. `make test` runs it, and so does CI when `docs/` changes.

The rest is yours: look at every page you changed as GitHub renders it, read it once as its section's reader would, and run every command and snippet you added or changed.
