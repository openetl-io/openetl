import { Code, type Transport } from "@connectrpc/connect";
import { createQueryOptions, useMutation, useQuery } from "@connectrpc/connect-query";
import { useQueryClient } from "@tanstack/react-query";
import { useNavigate } from "@tanstack/react-router";
import { toast } from "sonner";
import { AccessService, PrincipalType, type GetCallerResponse } from "@/gen/openetl/api/v1/access_pb";
import { errorCode, errorMessage } from "@/lib/api";
import { lastSegment } from "@/lib/names";

// Who is signed in, and what they may do on the org. The app's routes load it
// before they render.

export const callerQuery = (transport: Transport) => createQueryOptions(AccessService.method.getCaller, {}, { transport });

export function useCaller() {
  return useQuery(AccessService.method.getCaller, {}, { staleTime: 60_000 });
}

export const callerName = (c: GetCallerResponse) => c.principal?.displayName || lastSegment(c.principal?.name ?? "");

export const isUser = (c: GetCallerResponse) => c.principal?.type === PrincipalType.USER;

/** Signed in with a session, so the browser can sign out and change the password. */
export const hasSession = (c: GetCallerResponse) => !c.authless && c.credential.includes("/sessions/");

/** Ends the browser's session, then shows the sign-in page. */
export function useSignOut() {
  const queries = useQueryClient();
  const navigate = useNavigate();
  const done = () => {
    queries.clear();
    void navigate({ to: "/sign-in", search: { next: undefined } });
  };
  const signOut = useMutation(AccessService.method.deleteSession, {
    onSuccess: done,
    onError: (err) => (errorCode(err) === Code.Unauthenticated ? done() : toast(`Couldn't sign out: ${errorMessage(err)}`)),
  });
  return () => signOut.mutate({});
}
