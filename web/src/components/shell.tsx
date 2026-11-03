import { Fragment, useEffect, useState } from "react";
import { Outlet, useMatches, useNavigate, useParams, useRouter } from "@tanstack/react-router";
import { Search } from "lucide-react";
import { Toaster } from "sonner";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbPage,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";
import { Button } from "@/components/ui/button";
import { Kbd } from "@/components/ui/kbd";
import { Separator } from "@/components/ui/separator";
import { SidebarInset, SidebarProvider, SidebarTrigger } from "@/components/ui/sidebar";
import { TooltipProvider } from "@/components/ui/tooltip";
import { clearKey } from "@/lib/api";
import { usePrefs } from "@/lib/prefs";
import { AppSidebar } from "./app-sidebar";
import { CommandPalette } from "./command-palette";

export function Root() {
  const { dark } = usePrefs();
  return (
    <TooltipProvider delayDuration={250}>
      <Outlet />
      <Toaster theme={dark ? "dark" : "light"} position="bottom-right" />
    </TooltipProvider>
  );
}

/** Where the page sits: the workspace, then the crumbs its route declares. */
function Crumbs() {
  const matches = useMatches();
  const router = useRouter();
  const leaf = matches[matches.length - 1];
  const { workspace = "default" } = useParams({ strict: false });
  const crumbs = leaf?.staticData.crumbs?.(leaf.params as Record<string, string>) ?? [];
  return (
    <Breadcrumb className="min-w-0">
      <BreadcrumbList className="flex-nowrap">
        <BreadcrumbItem className="hidden sm:inline-flex">{workspace}</BreadcrumbItem>
        {crumbs.map((c, i) => (
          <Fragment key={i}>
            <BreadcrumbSeparator className={i === 0 ? "hidden sm:inline-flex" : undefined} />
            <BreadcrumbItem className="min-w-0">
              {c.to ? (
                <BreadcrumbLink
                  href={c.to}
                  onClick={(e) => {
                    e.preventDefault();
                    router.history.push(c.to!);
                  }}
                  className="truncate"
                >
                  {c.label}
                </BreadcrumbLink>
              ) : (
                <BreadcrumbPage className="truncate">{c.label}</BreadcrumbPage>
              )}
            </BreadcrumbItem>
          </Fragment>
        ))}
      </BreadcrumbList>
    </Breadcrumb>
  );
}

export function Shell() {
  const { workspace = "default" } = useParams({ strict: false });
  const navigate = useNavigate();
  const [palette, setPalette] = useState(false);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "k" && (e.metaKey || e.ctrlKey)) {
        e.preventDefault();
        setPalette((o) => !o);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);

  const signOut = () => {
    clearKey();
    void navigate({ to: "/sign-in", search: { next: undefined } });
  };

  return (
    <SidebarProvider defaultOpen={!document.cookie.includes("sidebar_state=false")}>
      <AppSidebar workspace={workspace} signOut={signOut} />
      <SidebarInset className="min-w-0">
        <header className="sticky top-0 z-20 flex h-12 shrink-0 items-center gap-2 border-b bg-background px-4">
          <SidebarTrigger className="-ml-1" />
          <Separator orientation="vertical" className="mr-1 data-[orientation=vertical]:h-4" />
          <Crumbs />
          <Button
            variant="outline"
            size="sm"
            onClick={() => setPalette(true)}
            className="ml-auto w-8 justify-start px-2 text-muted-foreground sm:w-60"
            aria-label="Search"
          >
            <Search aria-hidden />
            <span className="hidden sm:inline">Search</span>
            <Kbd className="ml-auto hidden sm:inline-flex">⌘K</Kbd>
          </Button>
        </header>
        <div className="mx-auto w-full max-w-[1600px] px-gutter pt-6 pb-16">
          <Outlet />
        </div>
      </SidebarInset>
      <CommandPalette open={palette} onOpenChange={setPalette} workspace={workspace} signOut={signOut} />
    </SidebarProvider>
  );
}
