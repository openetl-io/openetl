import { setNonce } from "get-nonce";

// openetld gives each index.html its CSP nonce, in Vite's meta tag, for the
// styles that CodeMirror and Radix's scroll lock insert.
export const cspNonce = document.querySelector<HTMLMetaElement>('meta[property="csp-nonce"]')?.nonce ?? "";

setNonce(cspNonce);
