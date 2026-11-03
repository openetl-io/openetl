import { create } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError, type Interceptor } from "@connectrpc/connect";
import { AccessService, GetCallerResponseSchema, PrincipalSchema, PrincipalType, Role, SessionSchema } from "@/gen/openetl/api/v1/access_pb";

// Sign-in in memory. ?mock-auth=none serves the authless mode, and
// ?mock-auth=signed-out starts signed out. robin@example.com signs in with
// "correct horse battery staple"; busy@example.com is rate limited. On
// /set-password, the token "used" is a used link.

const mode = new URLSearchParams(window.location.search).get("mock-auth");
const authless = mode === "none";
let signedIn = mode !== "signed-out";
let password = "correct horse battery staple";

export const user = create(PrincipalSchema, {
  name: "principals/robin",
  type: PrincipalType.USER,
  displayName: "Robin Ito",
  email: "robin@example.com",
  roleBindings: [{ role: Role.OWNER }],
  createTime: timestampFromMs(Date.now() - 41 * 24 * 3600_000),
});

const local = create(PrincipalSchema, {
  name: "principals/local",
  type: PrincipalType.USER,
  displayName: "Local",
  roleBindings: [{ role: Role.OWNER }],
});

const permissions = [
  "pipelines.get", "pipelines.pause", "pipelines.sync", "pipelines.delete", "syncs.get", "logs.get",
  "sources.get", "sources.check", "sources.discover", "sources.delete", "destinations.get", "destinations.check",
  "destinations.delete", "dbtProjects.get", "dbtProjects.delete", "secrets.get", "secrets.update", "secrets.delete",
  "webhooks.get", "webhooks.update", "webhooks.test", "webhooks.delete", "incidents.get", "incidents.resolve",
  "documents.apply", "connectors.apply", "principals.get", "principals.create", "principals.update",
  "principals.delete", "roles.grant", "keys.get", "keys.create", "keys.delete", "dataPlanes.get",
  "runnerTokens.get", "runnerTokens.create", "runnerTokens.delete", "audit.get", "owners.manage",
];

const session = () =>
  create(SessionSchema, {
    name: `${user.name}/sessions/0192f3a4-5b6c-7d8e-9f01-23456789abcd`,
    createTime: timestampFromMs(Date.now()),
    expireTime: timestampFromMs(Date.now() + 12 * 3600_000),
  });

const fail = (message: string, code: Code) => new ConnectError(message, code);

function checkPassword(p: string) {
  if ([...p].length < 15 || [...p].length > 256) throw fail("the password must be 15 to 256 characters", Code.InvalidArgument);
  if (p.toLowerCase().startsWith("password")) throw fail("that password is one of the most common; choose another", Code.InvalidArgument);
}

const open = new Set(["GetCaller", "CreateSession", "DeleteSession", "SetPassword"]);

/** Refuses the rest of the API while nobody is signed in, as the server would. */
export const requireSession: Interceptor = (next) => async (req) => {
  if (!authless && !signedIn && !open.has(req.method.name)) {
    throw fail("send an API key as Authorization: Bearer, or sign in", Code.Unauthenticated);
  }
  return next(req);
};

export const accessSession = {
  getCaller: () => {
    if (authless) return create(GetCallerResponseSchema, { principal: local, permissions, authless: true });
    if (!signedIn) throw fail("send an API key as Authorization: Bearer, or sign in", Code.Unauthenticated);
    const s = session();
    return create(GetCallerResponseSchema, { principal: user, credential: s.name, expireTime: s.expireTime, permissions });
  },
  createSession: ({ email, password: p }: { email: string; password: string }) => {
    if (email === "busy@example.com") throw fail("too many sign-ins from this address; try again in a minute", Code.ResourceExhausted);
    if (email !== user.email || p !== password) throw fail("wrong email or password", Code.Unauthenticated);
    signedIn = true;
    return { session: session() };
  },
  deleteSession: () => {
    signedIn = false;
    return {};
  },
  setPassword: ({ token, password: p }: { token: string; password: string }) => {
    if (token === "used") throw fail("the link isn't known; it may have been used, replaced or expired", Code.Unauthenticated);
    checkPassword(p);
    password = p;
    signedIn = true;
    return { session: session() };
  },
  changePassword: ({ currentPassword, newPassword }: { currentPassword: string; newPassword: string }) => {
    if (currentPassword !== password) throw fail("the current password is wrong", Code.InvalidArgument);
    checkPassword(newPassword);
    password = newPassword;
    return {};
  },
} satisfies Partial<Record<keyof typeof AccessService.method, unknown>>;
