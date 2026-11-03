import { createRootRoute, createRoute, createRouter, lazyRouteComponent, redirect } from "@tanstack/react-router";
import { hasKey } from "@/lib/api";
import { listSearch } from "@/lib/list-filter";
import { Root, Shell } from "@/components/shell";
import { SignIn } from "@/routes/sign-in";
import { tabs, type Tab } from "@/routes/tabs";
import { NotFound } from "@/routes/not-found";

const str = (v: unknown) => (typeof v === "string" && v !== "" ? v : undefined);

export type Crumb = { label: string; to?: string };

declare module "@tanstack/react-router" {
  interface StaticDataRouteOption {
    /** Where the page sits, after its workspace, for the top bar. */
    crumbs?: (params: Record<string, string>) => Crumb[];
  }
}

const root = createRootRoute({ component: Root, notFoundComponent: NotFound });

const signIn = createRoute({
  getParentRoute: () => root,
  path: "/sign-in",
  validateSearch: (s: Record<string, unknown>): { next?: string } => ({ next: str(s.next) }),
  component: SignIn,
});

const app = createRoute({
  getParentRoute: () => root,
  id: "_app",
  beforeLoad: ({ location }) => {
    if (!hasKey()) throw redirect({ to: "/sign-in", search: { next: location.href } });
  },
  component: Shell,
});

const index = createRoute({
  getParentRoute: () => app,
  path: "/",
  beforeLoad: () => {
    throw redirect({ to: "/workspaces/$workspace/pipelines", params: { workspace: "default" } });
  },
});

const ws = (p: Record<string, string>) => `/workspaces/${p.workspace}`;
const page = (label: string, path: string) => (p: Record<string, string>) => ({ label, to: `${ws(p)}/${path}` });

const pipelines = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/pipelines",
  validateSearch: listSearch(["status", "source", "destination"]),
  staticData: { crumbs: () => [{ label: "Pipelines" }] },
  component: lazyRouteComponent(() => import("@/routes/pipelines"), "PipelinesPage"),
});

const pipeline = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/pipelines/$pipeline",
  validateSearch: (s: Record<string, unknown>): { tab?: Tab; stream?: string; version?: string } => ({
    tab: tabs.find((t) => t === s.tab) as Tab | undefined,
    stream: str(s.stream),
    version: str(s.version),
  }),
  staticData: { crumbs: (p) => [page("Pipelines", "pipelines")(p), { label: p.pipeline! }] },
  component: lazyRouteComponent(() => import("@/routes/pipeline"), "PipelinePage"),
});

const sync = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/pipelines/$pipeline/syncs/$sync",
  staticData: {
    crumbs: (p) => [page("Pipelines", "pipelines")(p), page(p.pipeline!, `pipelines/${p.pipeline}`)(p), { label: `Sync ${p.sync!.slice(-8)}` }],
  },
  component: lazyRouteComponent(() => import("@/routes/sync"), "SyncPage"),
});

const incidents = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/incidents",
  validateSearch: listSearch(["state", "severity", "pipeline", "kind"]),
  staticData: { crumbs: () => [{ label: "Incidents" }] },
  component: lazyRouteComponent(() => import("@/routes/incidents"), "IncidentsPage"),
});

const incident = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/incidents/$pipeline/$incident",
  staticData: { crumbs: (p) => [page("Incidents", "incidents")(p), { label: `${p.pipeline} · ${p.incident!.slice(-8)}` }] },
  component: lazyRouteComponent(() => import("@/routes/incident"), "IncidentPage"),
});

const sources = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/sources",
  validateSearch: listSearch(["connector"]),
  staticData: { crumbs: () => [{ label: "Sources" }] },
  component: lazyRouteComponent(() => import("@/routes/connections"), "SourcesPage"),
});

const source = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/sources/$source",
  staticData: { crumbs: (p) => [page("Sources", "sources")(p), { label: p.source! }] },
  component: lazyRouteComponent(() => import("@/routes/connection"), "SourcePage"),
});

const destinations = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/destinations",
  validateSearch: listSearch(["connector"]),
  staticData: { crumbs: () => [{ label: "Destinations" }] },
  component: lazyRouteComponent(() => import("@/routes/connections"), "DestinationsPage"),
});

const destination = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/destinations/$destination",
  staticData: { crumbs: (p) => [page("Destinations", "destinations")(p), { label: p.destination! }] },
  component: lazyRouteComponent(() => import("@/routes/connection"), "DestinationPage"),
});

const dbtProjects = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/dbt-projects",
  validateSearch: listSearch([]),
  staticData: { crumbs: () => [{ label: "dbt projects" }] },
  component: lazyRouteComponent(() => import("@/routes/dbt-projects"), "DbtProjectsPage"),
});

const webhooks = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/webhooks",
  validateSearch: listSearch(["state"]),
  staticData: { crumbs: () => [{ label: "Webhooks" }] },
  component: lazyRouteComponent(() => import("@/routes/webhooks"), "WebhooksPage"),
});

const webhook = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/webhooks/$webhook",
  staticData: { crumbs: (p) => [page("Webhooks", "webhooks")(p), { label: p.webhook! }] },
  component: lazyRouteComponent(() => import("@/routes/webhook"), "WebhookPage"),
});

const secrets = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/secrets",
  validateSearch: listSearch([]),
  staticData: { crumbs: () => [{ label: "Secrets" }] },
  component: lazyRouteComponent(() => import("@/routes/secrets"), "SecretsPage"),
});

const access = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/access",
  validateSearch: (s: Record<string, unknown>): { q?: string; tab?: "tokens"; type?: string[]; role?: string[] } => ({
    ...listSearch(["type", "role"])(s),
    tab: s.tab === "tokens" ? "tokens" : undefined,
  }),
  staticData: { crumbs: () => [{ label: "Access" }] },
  component: lazyRouteComponent(() => import("@/routes/access"), "AccessPage"),
});

const principal = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/access/$principal",
  staticData: { crumbs: (p) => [page("Access", "access")(p), { label: p.principal! }] },
  component: lazyRouteComponent(() => import("@/routes/principal"), "PrincipalPage"),
});

const audit = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/audit",
  validateSearch: listSearch(["actor", "method", "result"]),
  staticData: { crumbs: () => [{ label: "Audit log" }] },
  component: lazyRouteComponent(() => import("@/routes/audit"), "AuditPage"),
});

export const router = createRouter({
  routeTree: root.addChildren([
    signIn,
    app.addChildren([
      index,
      pipelines,
      pipeline,
      sync,
      incidents,
      incident,
      sources,
      source,
      destinations,
      destination,
      dbtProjects,
      webhooks,
      webhook,
      secrets,
      access,
      principal,
      audit,
    ]),
  ]),
  defaultPreload: "intent",
  scrollRestoration: true,
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
