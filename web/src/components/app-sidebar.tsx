import { useState } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { Link, useLocation } from "@tanstack/react-router";
import { Bot, ChevronsUpDown, KeyRound, LogOut, ShieldOff, UserRound } from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuBadge,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
  useSidebar,
} from "@/components/ui/sidebar";
import type { GetCallerResponse } from "@/gen/openetl/api/v1/access_pb";
import { PipelineService } from "@/gen/openetl/api/v1/pipeline_pb";
import { callerName, hasSession, isUser, useCan, useCaller, useSignOut } from "@/lib/caller";
import { usePrefs, type Density, type Theme } from "@/lib/prefs";
import { bindingLabel } from "./access-fields";
import { ChangePasswordDialog } from "./change-password";
import { Logo } from "./logo";
import { visibleNav, type NavItem, type Page } from "./nav";

function NavGroup({ label, items, workspace, counts }: { label?: string; items: NavItem[]; workspace: string; counts: Partial<Record<Page, number>> }) {
  const { pathname } = useLocation();
  return (
    <SidebarGroup>
      {label && <SidebarGroupLabel>{label}</SidebarGroupLabel>}
      <SidebarGroupContent>
        <SidebarMenu>
          {items.map((it) => (
            <SidebarMenuItem key={it.label}>
              <SidebarMenuButton asChild tooltip={it.label} isActive={pathname.startsWith(it.to.replace("$workspace", workspace))}>
                <Link to={it.to} params={{ workspace }} search={{}}>
                  <it.icon />
                  <span>{it.label}</span>
                </Link>
              </SidebarMenuButton>
              {!!counts[it.to] && <SidebarMenuBadge className="text-failed">{counts[it.to]}</SidebarMenuBadge>}
            </SidebarMenuItem>
          ))}
        </SidebarMenu>
      </SidebarGroupContent>
    </SidebarGroup>
  );
}

function AccountTile({ caller }: { caller?: GetCallerResponse }) {
  const Icon = !caller ? UserRound : caller.authless ? ShieldOff : isUser(caller) ? UserRound : Bot;
  return (
    <span className="grid size-8 shrink-0 place-items-center rounded-md border bg-background">
      <Icon className="size-4" aria-hidden />
    </span>
  );
}

/** The caller's roles, highest first. */
const roles = (c: GetCallerResponse) => [...(c.principal?.roleBindings ?? [])].sort((a, b) => b.role - a.role).map(bindingLabel);

function Account() {
  const { isMobile } = useSidebar();
  const { theme, density, setTheme, setDensity } = usePrefs();
  const { data: caller } = useCaller();
  const signOut = useSignOut();
  const [changing, setChanging] = useState(false);
  const name = caller ? (caller.authless ? "Sign-in is off" : callerName(caller)) : "…";
  const detail = caller ? (caller.authless ? "Everyone here is an owner" : caller.principal?.email || roles(caller)[0] || "No role") : "";
  return (
    <SidebarMenu>
      <SidebarMenuItem>
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <SidebarMenuButton size="lg" className="data-[state=open]:bg-sidebar-accent">
              <AccountTile caller={caller} />
              <span className="grid min-w-0 flex-1 text-left leading-tight">
                <span className="truncate font-medium">{name}</span>
                <span className="truncate text-xs text-muted-foreground">{detail}</span>
              </span>
              <ChevronsUpDown className="ml-auto size-4 text-muted-foreground" aria-hidden />
            </SidebarMenuButton>
          </DropdownMenuTrigger>
          <DropdownMenuContent side={isMobile ? "top" : "right"} align="end" className="w-60">
            {caller && (
              <>
                <DropdownMenuLabel className="grid gap-0.5 font-normal">
                  <span className="truncate font-medium">{name}</span>
                  {caller.principal?.email && <span className="truncate text-muted-foreground">{caller.principal.email}</span>}
                  <span className="text-xs text-muted-foreground">{roles(caller).join(" · ") || "No role"}</span>
                </DropdownMenuLabel>
                <DropdownMenuSeparator />
              </>
            )}
            <DropdownMenuLabel className="text-xs text-muted-foreground">Theme</DropdownMenuLabel>
            <DropdownMenuRadioGroup value={theme} onValueChange={(v) => setTheme(v as Theme)}>
              <DropdownMenuRadioItem value="light">Light</DropdownMenuRadioItem>
              <DropdownMenuRadioItem value="dark">Dark</DropdownMenuRadioItem>
              <DropdownMenuRadioItem value="system">Match the system</DropdownMenuRadioItem>
            </DropdownMenuRadioGroup>
            <DropdownMenuSeparator />
            <DropdownMenuLabel className="text-xs text-muted-foreground">Rows</DropdownMenuLabel>
            <DropdownMenuRadioGroup value={density} onValueChange={(v) => setDensity(v as Density)}>
              <DropdownMenuRadioItem value="comfortable">Comfortable</DropdownMenuRadioItem>
              <DropdownMenuRadioItem value="compact">Compact</DropdownMenuRadioItem>
            </DropdownMenuRadioGroup>
            {caller && hasSession(caller) && (
              <>
                <DropdownMenuSeparator />
                {isUser(caller) && (
                  <DropdownMenuItem onSelect={() => setChanging(true)}>
                    <KeyRound />
                    Change password
                  </DropdownMenuItem>
                )}
                <DropdownMenuItem onSelect={signOut}>
                  <LogOut />
                  Sign out
                </DropdownMenuItem>
              </>
            )}
          </DropdownMenuContent>
        </DropdownMenu>
        <ChangePasswordDialog open={changing} onOpenChange={setChanging} />
      </SidebarMenuItem>
    </SidebarMenu>
  );
}

export function AppSidebar({ workspace }: { workspace: string }) {
  const pipelines = useQuery(
    PipelineService.method.listPipelines,
    { parent: `workspaces/${workspace}`, pageSize: 500 },
    { refetchInterval: 10_000 },
  );
  const incidents = pipelines.data?.pipelines.reduce((n, p) => n + (p.status?.openIncidents ?? 0), 0);
  const can = useCan(workspace);
  return (
    <Sidebar collapsible="icon">
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton size="lg" asChild className="group">
              <Link to="/workspaces/$workspace/pipelines" params={{ workspace }} search={{}}>
                <Logo className="size-8" />
                <span className="grid min-w-0 leading-tight">
                  <span className="truncate font-semibold tracking-tight">OpenETL</span>
                  <span className="truncate text-xs text-muted-foreground">{workspace}</span>
                </span>
              </Link>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>
      <SidebarContent>
        {visibleNav(can).map((g, i) => (
          <NavGroup key={i} label={g.label} items={g.items} workspace={workspace} counts={{ "/workspaces/$workspace/incidents": incidents }} />
        ))}
      </SidebarContent>
      <SidebarFooter>
        <Account />
      </SidebarFooter>
      <SidebarRail />
    </Sidebar>
  );
}
