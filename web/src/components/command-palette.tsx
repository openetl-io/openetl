import { useQuery } from "@connectrpc/connect-query";
import { useNavigate } from "@tanstack/react-router";
import { LogOut, Moon, Rows3, Workflow } from "lucide-react";
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { PipelineService } from "@/gen/openetl/api/v1/pipeline_pb";
import { lastSegment } from "@/lib/names";
import { usePrefs } from "@/lib/prefs";
import { pipelineStatus } from "@/lib/status";
import { nav } from "./nav";
import { StatusBadge } from "./status";

export function CommandPalette({
  open,
  onOpenChange,
  workspace,
  signOut,
}: {
  open: boolean;
  onOpenChange: (o: boolean) => void;
  workspace: string;
  signOut?: () => void;
}) {
  const navigate = useNavigate();
  const { dark, density, toggleTheme, toggleDensity } = usePrefs();
  const pipelines = useQuery(
    PipelineService.method.listPipelines,
    { parent: `workspaces/${workspace}`, pageSize: 500 },
    { enabled: open },
  );
  const run = (f: () => void) => () => {
    onOpenChange(false);
    f();
  };
  return (
    <CommandDialog open={open} onOpenChange={onOpenChange} title="Search" description="Jump to a page or a pipeline, or run a command">
      <CommandInput placeholder="Jump to a page or a pipeline, or run a command" />
      <CommandList>
        <CommandEmpty>Nothing matches.</CommandEmpty>
        <CommandGroup heading={`Pipelines in ${workspace}`}>
          {pipelines.data?.pipelines.map((p) => {
            const name = lastSegment(p.name);
            return (
              <CommandItem
                key={p.name}
                value={`pipeline ${name}`}
                onSelect={run(() =>
                  navigate({
                    to: "/workspaces/$workspace/pipelines/$pipeline",
                    params: { workspace, pipeline: name },
                    search: { tab: undefined, stream: undefined, version: undefined },
                  }),
                )}
              >
                <Workflow className="text-muted-foreground" />
                <span className="truncate">{name}</span>
                <StatusBadge status={pipelineStatus(p)} className="ml-auto text-xs" />
              </CommandItem>
            );
          })}
        </CommandGroup>
        <CommandGroup heading="Go to">
          {nav.flatMap((g) => g.items).map((it) => (
            <CommandItem key={it.to} value={`go ${it.label}`} onSelect={run(() => navigate({ to: it.to, params: { workspace }, search: {} }))}>
              <it.icon className="text-muted-foreground" />
              {it.label}
            </CommandItem>
          ))}
        </CommandGroup>
        <CommandGroup heading="Commands">
          <CommandItem onSelect={run(toggleTheme)}>
            <Moon className="text-muted-foreground" />
            {dark ? "Use the light theme" : "Use the dark theme"}
          </CommandItem>
          <CommandItem onSelect={run(toggleDensity)}>
            <Rows3 className="text-muted-foreground" />
            {density === "compact" ? "Use comfortable rows" : "Use compact rows"}
          </CommandItem>
          {signOut && (
            <CommandItem onSelect={run(signOut)}>
              <LogOut className="text-muted-foreground" />
              Sign out
            </CommandItem>
          )}
        </CommandGroup>
      </CommandList>
    </CommandDialog>
  );
}
