import { Field, FieldDescription, FieldError, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { maxPassword, minPassword, type NewPassword } from "@/lib/sign-in";

/** A new password and its confirmation. error is shown under the confirmation. */
export function NewPasswordFields({
  value,
  onChange,
  error,
  autoFocus,
}: {
  value: NewPassword;
  onChange: (v: NewPassword) => void;
  error?: string;
  autoFocus?: boolean;
}) {
  const invalid = error ? true : undefined;
  return (
    <>
      <Field data-invalid={invalid}>
        <FieldLabel htmlFor="new-password">New password</FieldLabel>
        <Input
          id="new-password"
          type="password"
          autoComplete="new-password"
          required
          autoFocus={autoFocus}
          value={value.password}
          onChange={(e) => onChange({ ...value, password: e.target.value })}
          aria-invalid={invalid}
          aria-describedby="new-password-hint"
        />
        <FieldDescription id="new-password-hint">
          {minPassword} to {maxPassword} characters. Not your email, and not a common password.
        </FieldDescription>
      </Field>
      <Field data-invalid={invalid}>
        <FieldLabel htmlFor="confirm-password">Confirm the new password</FieldLabel>
        <Input
          id="confirm-password"
          type="password"
          autoComplete="new-password"
          required
          value={value.confirm}
          onChange={(e) => onChange({ ...value, confirm: e.target.value })}
          aria-invalid={invalid}
          aria-describedby={error ? "new-password-error" : undefined}
        />
        <FieldError id="new-password-error">{error}</FieldError>
      </Field>
    </>
  );
}
