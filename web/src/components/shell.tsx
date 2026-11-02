import { useEffect, useState } from "react";
import { Link, Outlet, useNavigate, useParams } from "@tanstack/react-router";
import { ChevronRight, LogOut, Moon, Rows3, Rows4, Search, Sun } from "lucide-react";
import { Toaster } from "sonner";
import { Button } from "@/components/ui/button";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import { clearKey } from "@/lib/api";
import { usePrefs } from "@/lib/prefs";
import { CommandPalette } from "./command-palette";
import { Logo } from "./logo";

export function Root() {
  const { dark } = usePrefs();
  return (
    <TooltipProvider delayDuration={250}>
      <Outlet />
      <Toaster theme={dark ? "dark" : "light"} position="bottom-right" />
    </TooltipProvider>
  );
}

function IconButton({ label, onClick, children }: { label: string; onClick: () => void; children: React.ReactNode }) {
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button variant="ghost" size="icon-sm" onClick={onClick} aria-label={label}>
          {children}
        </Button>
      </TooltipTrigger>
      <TooltipContent>{label}</TooltipContent>
    </Tooltip>
  );
}

export function Shell() {
  const params = useParams({ strict: false });
  const workspace = params.workspace ?? "default";
  const navigate = useNavigate();
  const { dark, density, toggleTheme, toggleDensity } = usePrefs();
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
    <div className="min-h-dvh">
      <header className="sticky top-0 z-30 border-b bg-background/85 backdrop-blur-md">
        <div className="mx-auto flex h-12 max-w-[1400px] items-center gap-3 px-gutter">
          <Link
            to="/workspaces/$workspace/pipelines"
            params={{ workspace }}
            search={{ q: undefined, show: undefined }}
            className="group flex items-center gap-2 rounded-md font-semibold tracking-tight outline-offset-4 focus-visible:outline-2 focus-visible:outline-ring"
          >
            <Logo className="size-6" />
            <span>OpenETL</span>
          </Link>
          <nav aria-label="Breadcrumb" className="flex min-w-0 items-center gap-1 text-muted-foreground">
            <ChevronRight className="size-4 shrink-0 opacity-50" aria-hidden />
            <Tooltip>
              <TooltipTrigger asChild>
                <span className="truncate">{workspace}</span>
              </TooltipTrigger>
              <TooltipContent>Workspace</TooltipContent>
            </Tooltip>
            {params.pipeline && (
              <>
                <ChevronRight className="size-4 shrink-0 opacity-50" aria-hidden />
                <Link
                  to="/workspaces/$workspace/pipelines/$pipeline"
                  params={{ workspace, pipeline: params.pipeline }}
                  search={{ tab: undefined, stream: undefined, version: undefined }}
                  className="truncate hover:text-foreground"
                >
                  {params.pipeline}
                </Link>
              </>
            )}
          </nav>
          <div className="ml-auto flex items-center gap-1">
            <Button
              variant="outline"
              size="sm"
              onClick={() => setPalette(true)}
              className="mr-1 w-9 justify-start gap-2 px-2.5 text-muted-foreground sm:w-56"
              aria-label="Search pipelines"
            >
              <Search className="size-4" aria-hidden />
              <span className="hidden sm:inline">Search pipelines</span>
              <kbd className="ml-auto hidden rounded border bg-muted px-1.5 font-sans text-[0.6875rem] sm:inline">⌘K</kbd>
            </Button>
            <IconButton label={dark ? "Use the light theme" : "Use the dark theme"} onClick={toggleTheme}>
              {dark ? <Sun /> : <Moon />}
            </IconButton>
            <IconButton label={density === "compact" ? "Comfortable rows" : "Compact rows"} onClick={toggleDensity}>
              {density === "compact" ? <Rows3 /> : <Rows4 />}
            </IconButton>
            <IconButton label="Sign out" onClick={signOut}>
              <LogOut />
            </IconButton>
          </div>
        </div>
      </header>
      <main className="mx-auto max-w-[1400px] px-gutter pt-6 pb-16">
        <Outlet />
      </main>
      <CommandPalette open={palette} onOpenChange={setPalette} workspace={workspace} signOut={signOut} />
    </div>
  );
}
