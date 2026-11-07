import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { fileURLToPath, URL } from "node:url";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig, type Plugin } from "vite";

// In development the API runs on its own port (openetld's --listen) and Vite
// proxies the API's paths to it, so the UI and the API share an origin.
const api = process.env.OPENETL_API ?? "http://localhost:8080";

// openetld puts a fresh nonce here in every index.html it serves
// (internal/webui), for the styles CodeMirror and Vite insert.
const cspNonce = "OPENETL_CSP_NONCE";

const sha256 = (s: string) => `'sha256-${createHash("sha256").update(s).digest("base64")}'`;

// sonner inserts a fixed stylesheet without a nonce, so its hash goes in the
// CSP: csp.json, which openetld reads. It inserts an empty <style> first,
// hence the empty string's hash.
function sonnerStyleHash(): Plugin {
  return {
    name: "openetl-csp",
    apply: "build",
    generateBundle() {
      const src = readFileSync(fileURLToPath(new URL("./node_modules/sonner/dist/index.mjs", import.meta.url)), "utf8");
      const m = src.match(/__insertCSS\("((?:[^"\\]|\\.)*)"\)/);
      let css: string | undefined;
      try {
        css = m ? (JSON.parse(`"${m[1]}"`) as string) : undefined;
      } catch {
        css = undefined;
      }
      if (!css) this.error("sonner's stylesheet wasn't found in sonner/dist/index.mjs; the CSP needs its hash");
      this.emitFile({ type: "asset", fileName: "csp.json", source: JSON.stringify({ styleHashes: [sha256(css), sha256("")] }) + "\n" });
    },
  };
}

export default defineConfig({
  plugins: [react(), tailwindcss(), sonnerStyleHash()],
  resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } },
  html: { cspNonce },
  server: {
    proxy: {
      "/openetl.api.v1.": api,
      "/v1/": api,
      "/healthz": api,
      "/logos/": api,
    },
  },
  build: { chunkSizeWarningLimit: 700 },
});
