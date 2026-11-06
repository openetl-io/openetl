import { useState } from "react";
import { useMutation } from "@connectrpc/connect-query";
import { toast } from "sonner";
import { NewPasswordFields } from "@/components/new-password";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { AccessService } from "@/gen/openetl/api/v1/access_pb";
import { errorMessage } from "@/lib/api";
import { sentence } from "@/lib/format";
import { newPasswordProblem, type NewPassword } from "@/lib/sign-in";

const empty = { current: "", next: { password: "", confirm: "" } };

export function ChangePasswordDialog({ open, onOpenChange }: { open: boolean; onOpenChange: (o: boolean) => void }) {
  const [current, setCurrent] = useState(empty.current);
  const [next, setNext] = useState<NewPassword>(empty.next);
  const [problem, setProblem] = useState<string>();
  const change = useMutation(AccessService.method.changePassword, {
    onSuccess: () => {
      onOpenChange(false);
      toast("Password changed. You're signed out everywhere else.");
    },
  });
  const close = (o: boolean) => {
    if (!o) {
      setCurrent(empty.current);
      setNext(empty.next);
      setProblem(undefined);
      change.reset();
    }
    onOpenChange(o);
  };
  const error = problem ?? (change.error ? sentence(errorMessage(change.error)) : undefined);

  return (
    <Dialog open={open} onOpenChange={close}>
      <DialogContent className="sm:max-w-md">
        <form
          className="grid gap-6"
          onSubmit={(e) => {
            e.preventDefault();
            const p = newPasswordProblem(next);
            setProblem(p);
            if (!p) change.mutate({ currentPassword: current, newPassword: next.password });
          }}
        >
          <DialogHeader>
            <DialogTitle>Change your password</DialogTitle>
            <DialogDescription>Your other sessions end; this one stays signed in.</DialogDescription>
          </DialogHeader>
          <FieldGroup>
            <Field>
              <FieldLabel htmlFor="current-password">Current password</FieldLabel>
              <Input
                id="current-password"
                type="password"
                autoComplete="current-password"
                required
                autoFocus
                value={current}
                onChange={(e) => {
                  setCurrent(e.target.value);
                  change.reset();
                }}
              />
            </Field>
            <NewPasswordFields
              value={next}
              onChange={(v) => {
                setNext(v);
                setProblem(undefined);
                change.reset();
              }}
              error={error}
            />
          </FieldGroup>
          <DialogFooter>
            <DialogClose asChild>
              <Button type="button" variant="outline">
                Cancel
              </Button>
            </DialogClose>
            <Button type="submit" disabled={change.isPending || current === "" || next.password === ""}>
              {change.isPending ? "Changing…" : "Change password"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
