import { useState } from "react";
import { Code } from "@connectrpc/connect";
import { useMutation } from "@connectrpc/connect-query";
import { useQueryClient } from "@tanstack/react-query";
import { useNavigate, useSearch } from "@tanstack/react-router";
import { AuthLayout, RememberMe } from "@/components/auth-layout";
import { Button } from "@/components/ui/button";
import { Field, FieldError, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { AccessService } from "@/gen/openetl/api/v1/access_pb";
import { errorCode, errorMessage } from "@/lib/api";
import { sentence } from "@/lib/format";
import { afterSignIn } from "@/lib/sign-in";

function signInError(err: unknown) {
  switch (errorCode(err)) {
    case Code.Unauthenticated:
      return "Wrong email or password. After too many tries the account locks for a while; an admin can send you a link to set a new password.";
    case Code.ResourceExhausted:
      return "Too many sign-ins from this address. Try again in a minute.";
    default:
      return `Couldn't sign in: ${sentence(errorMessage(err))}`;
  }
}

export function SignIn() {
  const { next } = useSearch({ from: "/sign-in" });
  const navigate = useNavigate();
  const queries = useQueryClient();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [remember, setRemember] = useState(false);
  const signIn = useMutation(AccessService.method.createSession, {
    onSuccess: () => {
      queries.clear();
      void navigate({ to: afterSignIn(next) });
    },
  });
  const error = signIn.error ? signInError(signIn.error) : undefined;

  return (
    <AuthLayout title="Sign in to OpenETL">
      <form
        onSubmit={(e) => {
          e.preventDefault();
          signIn.mutate({ email: email.trim(), password, remember });
        }}
      >
        <FieldGroup>
          <Field data-invalid={error ? true : undefined}>
            <FieldLabel htmlFor="email">Email</FieldLabel>
            <Input
              id="email"
              type="email"
              autoComplete="username"
              required
              autoFocus
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              aria-invalid={error ? true : undefined}
            />
          </Field>
          <Field data-invalid={error ? true : undefined}>
            <FieldLabel htmlFor="password">Password</FieldLabel>
            <Input
              id="password"
              type="password"
              autoComplete="current-password"
              required
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              aria-invalid={error ? true : undefined}
              aria-describedby={error ? "sign-in-error" : undefined}
            />
            <FieldError id="sign-in-error">{error}</FieldError>
          </Field>
          <RememberMe checked={remember} onChange={setRemember} />
          <Button type="submit" disabled={signIn.isPending || email.trim() === "" || password === ""}>
            {signIn.isPending ? "Signing in…" : "Sign in"}
          </Button>
        </FieldGroup>
      </form>
    </AuthLayout>
  );
}
