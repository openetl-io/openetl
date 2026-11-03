import "@/lib/csp";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { TransportProvider } from "@connectrpc/connect-query";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider } from "@tanstack/react-router";
import { Code } from "@connectrpc/connect";
import { errorCode, transport, whenSignedOut } from "@/lib/api";
import { router } from "@/router";
import "./index.css";

const queries = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5_000,
      retry: (n, err) =>
        n < 2 && ![Code.Unauthenticated, Code.PermissionDenied, Code.NotFound, Code.InvalidArgument].includes(errorCode(err)),
    },
  },
});

// pnpm dev:mock serves the API from fixtures; production builds drop them.
const api = import.meta.env.VITE_MOCK_API ? (await import("@/mocks/transport")).mockTransport : transport;

const outside = ["/sign-in", "/set-password"];

whenSignedOut(() => {
  queries.clear();
  const { pathname, href } = router.state.location;
  if (!outside.includes(pathname)) void router.navigate({ to: "/sign-in", search: { next: href } });
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <TransportProvider transport={api}>
      <QueryClientProvider client={queries}>
        <RouterProvider router={router} context={{ queries, transport: api }} />
      </QueryClientProvider>
    </TransportProvider>
  </StrictMode>,
);
