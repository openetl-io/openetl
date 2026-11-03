import { CircleCheck, CirclePause } from "lucide-react";
import { PrincipalType, Role, type Principal, type RoleBinding } from "@/gen/openetl/api/v1/access_pb";
import { enumLabel } from "@/lib/format";
import { lastSegment } from "@/lib/names";

export const typeLabel = (t: PrincipalType) => (t === PrincipalType.SERVICE_ACCOUNT ? "Service account" : "User");
export const roleLabel = (r: Role) => enumLabel(Role[r]);

/** "Admin" for the whole org, "Operator in default" for one workspace. */
export function bindingLabel(b: RoleBinding) {
  return b.workspace ? `${roleLabel(b.role)} in ${lastSegment(b.workspace)}` : roleLabel(b.role);
}

/** A principal's highest role, for sorting and filtering. */
export const topRole = (p: Principal) => Math.max(0, ...p.roleBindings.map((b) => b.role));

export function PrincipalStatus({ disabled }: { disabled: boolean }) {
  return disabled ? (
    <span className="inline-flex items-center gap-1.5 text-muted-foreground">
      <CirclePause className="size-4 shrink-0" aria-hidden />
      Disabled
    </span>
  ) : (
    <span className="inline-flex items-center gap-1.5">
      <CircleCheck className="size-4 shrink-0 text-healthy" aria-hidden />
      Active
    </span>
  );
}
