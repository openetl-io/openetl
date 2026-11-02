import { fileURLToPath, URL } from "node:url";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";

// In development the API runs on its own port (openetld's --listen) and Vite
// proxies the API's paths to it, so the UI and the API share an origin.
const api = process.env.OPENETL_API ?? "http://localhost:8080";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: { alias: { "@": fileURLToPath(new URL("./src", import.meta.url)) } },
  server: {
    proxy: {
      "/openetl.api.v1.": api,
      "/v1/": api,
      "/healthz": api,
    },
  },
  build: { chunkSizeWarningLimit: 700 },
});
