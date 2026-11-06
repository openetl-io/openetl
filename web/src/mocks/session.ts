import { create } from "@bufbuild/protobuf";
import { timestampFromMs } from "@bufbuild/protobuf/wkt";
import { Code, ConnectError, type Interceptor } from "@connectrpc/connect";
import { AccessService, GetCallerResponseSchema, PrincipalSchema, PrincipalType, Role, SessionSchema } from "@/gen/openetl/api/v1/access_pb";

// Sign-in in memory. ?mock-auth=none serves the authless mode, and
// ?mock-auth=signed-out starts signed out. robin@example.com signs in with
// "correct horse battery staple"; busy@example.com is rate limited. On
// /set-password, the token "used" is a used link. ?mock-role=viewer (or
// operator, editor, admin) signs Robin in with that role instead of owner.

const params = new URLSearchParams(window.location.search);
const mode = params.get("mock-auth");
const authless = mode === "none";
let signedIn = mode !== "signed-out";
let password = "correct horse battery staple";

// What each role may do, by the least role that may, as internal/api/authz.go has it.
const roles = ["viewer", "operator", "editor", "admin", "owner"] as const;
type RoleName = (typeof roles)[number];
const least: Record<string, RoleName> = {
  "pipelines.get": "viewer", "pipelines.pause": "operator", "pipelines.sync": "operator", "pipelines.delete": "editor",
  "syncs.get": "viewer", "logs.get": "viewer", "sources.get": "viewer", "sources.check": "operator",
  "sources.discover": "operator", "sources.delete": "editor", "destinations.get": "viewer", "destinations.check": "operator",
  "destinations.delete": "editor", "dbtProjects.get": "viewer", "dbtProjects.delete": "editor", "secrets.get": "viewer",
  "secrets.update": "editor", "secrets.delete": "editor", "webhooks.get": "viewer", "webhooks.update": "operator",
  "webhooks.test": "operator", "webhooks.delete": "editor", "incidents.get": "viewer", "incidents.resolve": "operator",
  "documents.apply": "editor", "connectors.apply": "admin", "principals.get": "viewer", "principals.create": "admin",
  "principals.update": "admin", "principals.delete": "admin", "roles.grant": "admin", "keys.get": "viewer",
  "keys.create": "admin", "keys.delete": "admin", "dataPlanes.get": "viewer", "runnerTokens.get": "viewer",
  "runnerTokens.create": "admin", "runnerTokens.delete": "admin", "audit.get": "admin", "sessions.revoke": "admin",
  "passwords.reset": "admin", "owners.manage": "owner",
};
const permitted = (r: RoleName) => Object.keys(least).filter((p) => roles.indexOf(least[p]!) <= roles.indexOf(r));
const role = roles.find((r) => r === params.get("mock-role")) ?? "owner";
const permissions = permitted(role);

export const user = create(PrincipalSchema, {
  name: "principals/robin",
  type: PrincipalType.USER,
  displayName: "Robin Ito",
  email: "robin@example.com",
  roleBindings: [{ role: Role[role.toUpperCase() as Uppercase<RoleName>] }],
  createTime: timestampFromMs(Date.now() - 41 * 24 * 3600_000),
});

const local = create(PrincipalSchema, {
  name: "principals/local",
  type: PrincipalType.USER,
  displayName: "Local",
  roleBindings: [{ role: Role.OWNER }],
});

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
    if (authless) return create(GetCallerResponseSchema, { principal: local, permissions: permitted("owner"), authless: true });
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
  revokeSessions: ({ name }: { name: string }) => {
    if (name === user.name) signedIn = false;
    return {};
  },
  changePassword: ({ currentPassword, newPassword }: { currentPassword: string; newPassword: string }) => {
    if (currentPassword !== password) throw fail("the current password is wrong", Code.InvalidArgument);
    checkPassword(newPassword);
    password = newPassword;
    return {};
  },
} satisfies Partial<Record<keyof typeof AccessService.method, unknown>>;
