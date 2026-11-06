import { Code, ConnectError, type Interceptor } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { AccessService } from "@/gen/openetl/api/v1/access_pb";

// The browser's session cookie authenticates every call; the page holds no
// credential.

let onSignedOut: () => void = () => {};

export function whenSignedOut(f: () => void) {
  onSignedOut = f;
}

// These answer UNAUTHENTICATED for a wrong password or an ended session,
// which the pages calling them handle.
const ownsUnauthenticated = new Set<string>([
  AccessService.method.getCaller.name,
  AccessService.method.createSession.name,
  AccessService.method.deleteSession.name,
  AccessService.method.setPassword.name,
  AccessService.method.changePassword.name,
]);

export const signedOut: Interceptor = (next) => async (req) => {
  try {
    return await next(req);
  } catch (err) {
    const own = req.service.typeName === AccessService.typeName && ownsUnauthenticated.has(req.method.name);
    if (!own && ConnectError.from(err).code === Code.Unauthenticated) onSignedOut();
    throw err;
  }
};

export const transport = createConnectTransport({
  baseUrl: window.location.origin,
  interceptors: [signedOut],
});

export function errorMessage(err: unknown): string {
  return ConnectError.from(err).rawMessage;
}

export function errorCode(err: unknown): Code {
  return ConnectError.from(err).code;
}
