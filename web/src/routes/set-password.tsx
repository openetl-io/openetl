import { useEffect, useState } from "react";
import { Code } from "@connectrpc/connect";
import { useMutation } from "@connectrpc/connect-query";
import { useQueryClient } from "@tanstack/react-query";
import { Link, useNavigate } from "@tanstack/react-router";
import { CircleAlert } from "lucide-react";
import { AuthLayout, RememberMe } from "@/components/auth-layout";
import { NewPasswordFields } from "@/components/new-password";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { FieldGroup } from "@/components/ui/field";
import { AccessService } from "@/gen/openetl/api/v1/access_pb";
import { errorCode, errorMessage } from "@/lib/api";
import { sentence } from "@/lib/format";
import { newPasswordProblem, type NewPassword } from "@/lib/sign-in";

const askAgain = "Ask an admin for a new link: they make one with openetl principal password-link.";

function LinkProblem({ title }: { title: string }) {
  return (
    <div className="grid gap-4">
      <Alert variant="destructive">
        <CircleAlert aria-hidden />
        <AlertTitle>{title}</AlertTitle>
        <AlertDescription>{askAgain}</AlertDescription>
      </Alert>
      <Link to="/sign-in" search={{ next: undefined }} className="text-signal underline-offset-4 hover:underline">
        Sign in with a password you have
      </Link>
    </div>
  );
}

/** Sets a password from the one-use link an admin made. The token is the link's fragment. */
export function SetPassword() {
  const navigate = useNavigate();
  const queries = useQueryClient();
  const [token] = useState(() => window.location.hash.slice(1));
  const [value, setValue] = useState<NewPassword>({ password: "", confirm: "" });
  const [remember, setRemember] = useState(false);
  const [problem, setProblem] = useState<string>();

  // Off the address bar and out of the history as soon as it's read.
  useEffect(() => {
    if (window.location.hash) window.history.replaceState(window.history.state, "", window.location.pathname + window.location.search);
  }, []);

  const set = useMutation(AccessService.method.setPassword, {
    onSuccess: () => {
      queries.clear();
      void navigate({ to: "/" });
    },
  });

  if (!token) {
    return (
      <AuthLayout title="Set your password">
        <LinkProblem title="This page needs the link an admin made for you" />
      </AuthLayout>
    );
  }

  const code = set.error ? errorCode(set.error) : undefined;
  if (code === Code.Unauthenticated || code === Code.FailedPrecondition || code === Code.NotFound) {
    return (
      <AuthLayout title="Set your password">
        <LinkProblem title="This link has expired or was already used" />
      </AuthLayout>
    );
  }
  const serverProblem =
    code === Code.InvalidArgument
      ? sentence(errorMessage(set.error))
      : set.error
        ? `Couldn't set the password: ${sentence(errorMessage(set.error))}`
        : undefined;

  return (
    <AuthLayout title="Set your password" description="Choose the password you'll sign in to OpenETL with.">
      <form
        onSubmit={(e) => {
          e.preventDefault();
          const p = newPasswordProblem(value);
          setProblem(p);
          if (!p) set.mutate({ token, password: value.password, remember });
        }}
      >
        <FieldGroup>
          <NewPasswordFields
            value={value}
            onChange={(v) => {
              setValue(v);
              setProblem(undefined);
            }}
            error={problem ?? serverProblem}
            autoFocus
          />
          <RememberMe checked={remember} onChange={setRemember} />
          <Button type="submit" disabled={set.isPending || value.password === ""}>
            {set.isPending ? "Setting it…" : "Set password and sign in"}
          </Button>
        </FieldGroup>
      </form>
    </AuthLayout>
  );
}
