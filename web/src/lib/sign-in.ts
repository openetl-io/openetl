// The rules the UI checks before the server sees a password; the
// server checks them again, and also refuses common passwords.

export const minPassword = 15;
export const maxPassword = 256;

export type NewPassword = { password: string; confirm: string };

/** What's wrong with a new password, if anything. */
export function newPasswordProblem({ password, confirm }: NewPassword): string | undefined {
  const n = [...password].length;
  if (n < minPassword) return `Use at least ${minPassword} characters; a few words in a row is easy to remember.`;
  if (n > maxPassword) return `Use at most ${maxPassword} characters.`;
  if (password !== confirm) return "The two passwords don't match.";
  return undefined;
}

/** Where to go after signing in: a path in the app, never back to sign-in or another origin. */
export function afterSignIn(next: string | undefined) {
  return next && next.startsWith("/") && !next.startsWith("//") && !next.startsWith("/\\") && !next.startsWith("/sign-in") ? next : "/";
}
