# Working on the UI

The UI is a client of openetl's API and nothing else: React, TypeScript, Tailwind and shadcn components, built with Vite. [The repository's AGENTS.md](../AGENTS.md) applies here too.

- **No custom CSS.** Use Tailwind utilities, and a shadcn component wherever one fits, left as shipped. `src/index.css` holds only the theme's tokens. Markup that repeats becomes a component in `src/components`.
- **Adding a shadcn component:** `yes n | pnpm dlx shadcn@latest add NAME`. The CLI can write `import { cn } from "cn"` and add the unrelated npm package `cn`; if it does, `pnpm remove cn` and import `cn` from `@/lib/utils`. Check `git diff package.json` after.
- **No formatter is configured.** The code is formatted by hand, at about 140 columns; don't run Prettier over it, which reflows whole files. The checks are `pnpm typecheck` (`tsc -b`) and `pnpm test` (vitest).
- `src/gen` is generated from `proto/` by `make generate` at the repository's root. Never edit it.
- `pnpm dev:mock` serves the UI against the mock API in `src/mocks`, with no server running. Look at a change in a browser, or in headless Chrome's screenshot, as well as checking types: a page can type-check and still read wrong.
- `pnpm dev` proxies to the API on `localhost:8080` (`make dev` at the root).
