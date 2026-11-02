import { useState } from "react";
import { createClient, Code } from "@connectrpc/connect";
import { useNavigate, useSearch } from "@tanstack/react-router";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Logo } from "@/components/logo";
import { PipelineService } from "@/gen/openetl/api/v1/pipeline_pb";
import { errorCode, errorMessage, storeKey, transport, clearKey } from "@/lib/api";

export function SignIn() {
  const { next } = useSearch({ from: "/sign-in" });
  const navigate = useNavigate();
  const [key, setKey] = useState("");
  const [remember, setRemember] = useState(false);
  const [error, setError] = useState<string>();
  const [busy, setBusy] = useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError(undefined);
    storeKey(key.trim(), remember);
    try {
      await createClient(PipelineService, transport).listPipelines({ parent: "workspaces/default", pageSize: 1 });
    } catch (err) {
      // A key without access to the default workspace is still a key.
      const code = errorCode(err);
      if (code === Code.Unauthenticated) {
        setError("That key isn't valid. Keys start with oetl_ and are printed once, when they're created.");
        clearKey();
        setBusy(false);
        return;
      }
      if (code !== Code.PermissionDenied && code !== Code.NotFound) {
        setError(`Couldn't reach the API: ${errorMessage(err)}`);
        clearKey();
        setBusy(false);
        return;
      }
    }
    const to = next && next.startsWith("/") && !next.startsWith("/sign-in") ? next : "/";
    void navigate({ to });
  };

  return (
    <div className="grid min-h-dvh place-items-center px-4">
      <form onSubmit={submit} className="grid w-full max-w-sm gap-6">
        <div className="grid justify-items-start gap-4">
          <Logo motion="intro" className="size-12" />
          <div className="grid gap-1">
            <h1 className="text-xl font-semibold tracking-tight">Sign in to OpenETL</h1>
            <p className="text-muted-foreground">
              With an API key. The server wrote its owner's key to <code className="font-mono text-[0.8125rem]">.openetl/admin.key</code> on
              first start, and <code className="font-mono text-[0.8125rem]">openetl key create</code> makes more.
            </p>
          </div>
        </div>
        <div className="grid gap-2">
          <label htmlFor="key" className="font-medium">
            API key
          </label>
          <Input
            id="key"
            type="password"
            autoComplete="current-password"
            spellCheck={false}
            required
            autoFocus
            value={key}
            onChange={(e) => setKey(e.target.value)}
            aria-invalid={error ? true : undefined}
            aria-describedby={error ? "key-error" : undefined}
            className="font-mono"
          />
          {error && (
            <p id="key-error" className="text-failed">
              {error}
            </p>
          )}
          <label className="flex items-center gap-2 text-muted-foreground">
            <input type="checkbox" checked={remember} onChange={(e) => setRemember(e.target.checked)} className="size-4 accent-signal" />
            Keep me signed in on this device
          </label>
        </div>
        <Button type="submit" disabled={busy || key.trim() === ""}>
          {busy ? "Signing in…" : "Sign in"}
        </Button>
      </form>
    </div>
  );
}
