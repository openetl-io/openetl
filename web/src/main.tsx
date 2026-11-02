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

whenSignedOut(() => {
  queries.clear();
  void router.navigate({ to: "/sign-in", search: { next: router.state.location.href } });
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <TransportProvider transport={transport}>
      <QueryClientProvider client={queries}>
        <RouterProvider router={router} />
      </QueryClientProvider>
    </TransportProvider>
  </StrictMode>,
);
