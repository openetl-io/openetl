import { Link } from "@tanstack/react-router";
import { KeyRound } from "lucide-react";
import { DescriptionItem, DescriptionList } from "@/components/detail-header";
import type { Setting } from "@/lib/clusters";
import { cn } from "@/lib/utils";

/** A secret by name, linked to the secrets page filtered to it. */
export function SecretName({ workspace, name }: { workspace: string; name: string }) {
  return (
    <Link to="/workspaces/$workspace/secrets" params={{ workspace }} search={{ q: name }} className="inline-flex items-center gap-1.5 hover:underline">
      <KeyRound className="size-3.5 text-muted-foreground" aria-hidden />
      <span className="font-mono text-[0.8125rem] break-all">{name}</span>
      <span className="sr-only">(secret)</span>
    </Link>
  );
}

function SettingValue({ workspace, setting: s }: { workspace: string; setting: Setting }) {
  if (s.secret) return <SecretName workspace={workspace} name={s.secret} />;
  if (!s.value) return <span className="text-muted-foreground">{s.unset ?? "—"}</span>;
  return <span className={cn(s.mono && "font-mono text-[0.8125rem] break-all")}>{s.value}</span>;
}

/** A cluster's settings, each with what applies when it's unset. */
export function ClusterSettings({ workspace, settings }: { workspace: string; settings: Setting[] }) {
  return (
    <DescriptionList className="max-w-4xl grid-cols-1 sm:grid-cols-2 lg:grid lg:grid-cols-3">
      {settings.map((s) => (
        <DescriptionItem key={s.label} label={s.label}>
          <span className="grid min-w-0 gap-0.5 whitespace-normal">
            <span>
              <SettingValue workspace={workspace} setting={s} />
            </span>
            {s.note && <span className="text-[0.8125rem] text-muted-foreground">{s.note}</span>}
          </span>
        </DescriptionItem>
      ))}
    </DescriptionList>
  );
}
