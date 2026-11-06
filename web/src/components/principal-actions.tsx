import { LogOut } from "lucide-react";
import { Button } from "@/components/ui/button";
import { AccessService, PrincipalType, Role, type Principal } from "@/gen/openetl/api/v1/access_pb";
import { useAction } from "@/lib/action";
import { useCan, useCaller } from "@/lib/caller";
import { lastSegment } from "@/lib/names";
import { Confirm } from "./confirm";

/** Signing a user out of every browser, for a role that may. An owner needs an owner. */
export function PrincipalActions({ principal }: { principal: Principal }) {
  const can = useCan();
  const { data: caller } = useCaller();
  const who = principal.displayName || lastSegment(principal.name);
  const self = caller?.principal?.name === principal.name;
  // Signing yourself out ends this session too, and the reload's refusal shows the sign-in page.
  const revoke = useAction(AccessService.method.revokeSessions, { done: `${who} is signed out everywhere`, failed: "Couldn't sign out", refresh: self ? [AccessService] : [] });
  const owner = principal.roleBindings.some((b) => b.role === Role.OWNER);
  if (principal.type !== PrincipalType.USER || !can("sessions.revoke") || (owner && !can("owners.manage"))) return null;

  return (
    <Confirm
      title={`Sign ${who} out everywhere?`}
      description={`Every browser signed in as ${self ? "you, this one too," : who} is signed out. API keys keep working.`}
      confirm={revoke.isPending ? "Signing out…" : "Sign out everywhere"}
      destructive
      pending={revoke.isPending}
      onConfirm={(close) => revoke.mutate({ name: principal.name }, { onSuccess: close })}
    >
      <Button size="sm" variant="outline">
        <LogOut aria-hidden />
        Sign out everywhere
      </Button>
    </Confirm>
  );
}
