import { Boxes, Database, KeyRound, Plug, ScrollText, Siren, Users, Warehouse, Webhook, Workflow, type LucideIcon } from "lucide-react";

export type Page =
  | "/workspaces/$workspace/pipelines"
  | "/workspaces/$workspace/incidents"
  | "/workspaces/$workspace/sources"
  | "/workspaces/$workspace/destinations"
  | "/workspaces/$workspace/dbt-projects"
  | "/workspaces/$workspace/connectors"
  | "/workspaces/$workspace/webhooks"
  | "/workspaces/$workspace/secrets"
  | "/workspaces/$workspace/access"
  | "/workspaces/$workspace/audit";

/** permission, when set, is what the caller needs to see the item. */
export type NavItem = { label: string; icon: LucideIcon; to: Page; permission?: string };

/** The sidebar's groups, which the command palette also offers. */
export const nav: { label?: string; items: NavItem[] }[] = [
  {
    items: [
      { label: "Pipelines", icon: Workflow, to: "/workspaces/$workspace/pipelines" },
      { label: "Incidents", icon: Siren, to: "/workspaces/$workspace/incidents" },
    ],
  },
  {
    label: "Connections",
    items: [
      { label: "Sources", icon: Database, to: "/workspaces/$workspace/sources" },
      { label: "Destinations", icon: Warehouse, to: "/workspaces/$workspace/destinations" },
      { label: "dbt projects", icon: Boxes, to: "/workspaces/$workspace/dbt-projects" },
      { label: "Connectors", icon: Plug, to: "/workspaces/$workspace/connectors" },
    ],
  },
  {
    label: "Settings",
    items: [
      { label: "Webhooks", icon: Webhook, to: "/workspaces/$workspace/webhooks" },
      { label: "Secrets", icon: KeyRound, to: "/workspaces/$workspace/secrets" },
      { label: "Access", icon: Users, to: "/workspaces/$workspace/access" },
      { label: "Audit log", icon: ScrollText, to: "/workspaces/$workspace/audit", permission: "audit.get" },
    ],
  },
];

/** The groups' items the caller may see, without the groups left empty. */
export function visibleNav(can: (permission: string) => boolean) {
  return nav
    .map((g) => ({ ...g, items: g.items.filter((it) => !it.permission || can(it.permission)) }))
    .filter((g) => g.items.length > 0);
}
