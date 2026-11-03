import type { DescMessage, DescMethodUnary, DescService, MessageShape } from "@bufbuild/protobuf";
import { createConnectQueryKey, useMutation } from "@connectrpc/connect-query";
import { useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { errorMessage } from "@/lib/api";

/** A call that changes something: it says how it went, then reloads what the services serve. */
export function useAction<I extends DescMessage, O extends DescMessage>(
  method: DescMethodUnary<I, O>,
  { done, failed, refresh }: { done: string | ((res: MessageShape<O>) => string); failed: string; refresh: DescService[] },
) {
  const queries = useQueryClient();
  return useMutation(method, {
    onSuccess: async (res) => {
      toast(typeof done === "string" ? done : done(res));
      await Promise.all(refresh.map((s) => queries.invalidateQueries({ queryKey: createConnectQueryKey({ schema: s, cardinality: undefined }) })));
    },
    onError: (err) => toast.error(`${failed}: ${errorMessage(err)}`),
  });
}
