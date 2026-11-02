import { createRootRoute, createRoute, createRouter, lazyRouteComponent, redirect } from "@tanstack/react-router";
import { hasKey } from "@/lib/api";
import { Root, Shell } from "@/components/shell";
import { SignIn } from "@/routes/sign-in";
import { tabs, type Tab } from "@/routes/tabs";
import { NotFound } from "@/routes/not-found";

const str = (v: unknown) => (typeof v === "string" && v !== "" ? v : undefined);

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

export type PipelineFilter = "all" | "failing" | "running" | "paused";

const pipelines = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/pipelines",
  validateSearch: (s: Record<string, unknown>): { q?: string; show?: PipelineFilter } => ({
    q: str(s.q),
    show: (["failing", "running", "paused"] as const).find((f) => f === s.show) as PipelineFilter | undefined,
  }),
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
  component: lazyRouteComponent(() => import("@/routes/pipeline"), "PipelinePage"),
});

const sync = createRoute({
  getParentRoute: () => app,
  path: "/workspaces/$workspace/pipelines/$pipeline/syncs/$sync",
  component: lazyRouteComponent(() => import("@/routes/sync"), "SyncPage"),
});

export const router = createRouter({
  routeTree: root.addChildren([signIn, app.addChildren([index, pipelines, pipeline, sync])]),
  defaultPreload: "intent",
  scrollRestoration: true,
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
