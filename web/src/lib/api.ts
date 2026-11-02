import { Code, ConnectError, type Interceptor } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";

// The API key lives in sessionStorage, or in localStorage when the viewer
// asks to be remembered on this device.
const keyName = "openetl.key";

export function readKey(): string | null {
  try {
    return sessionStorage.getItem(keyName) ?? localStorage.getItem(keyName);
  } catch {
    return null;
  }
}

export function storeKey(key: string, remember: boolean) {
  try {
    sessionStorage.setItem(keyName, key);
    if (remember) localStorage.setItem(keyName, key);
  } catch {
    // Without storage the key lasts until the page reloads.
  }
  memoryKey = key;
}

export function clearKey() {
  memoryKey = null;
  try {
    sessionStorage.removeItem(keyName);
    localStorage.removeItem(keyName);
  } catch {
    // Nothing stored.
  }
}

let memoryKey: string | null = readKey();
let onSignedOut: () => void = () => {};

export function whenSignedOut(f: () => void) {
  onSignedOut = f;
}

export function hasKey() {
  return memoryKey !== null;
}

const auth: Interceptor = (next) => async (req) => {
  if (memoryKey) req.header.set("Authorization", `Bearer ${memoryKey}`);
  try {
    return await next(req);
  } catch (err) {
    if (ConnectError.from(err).code === Code.Unauthenticated) {
      clearKey();
      onSignedOut();
    }
    throw err;
  }
};

export const transport = createConnectTransport({
  baseUrl: window.location.origin,
  interceptors: [auth],
});

export function errorMessage(err: unknown): string {
  return ConnectError.from(err).rawMessage;
}

export function errorCode(err: unknown): Code {
  return ConnectError.from(err).code;
}
