import { Code } from "@connectrpc/connect";
import { Lock } from "lucide-react";
import { errorCode, errorMessage } from "@/lib/api";
import { sentence } from "@/lib/format";
import { ErrorNote } from "./empty";
import { EmptyState } from "./empty-state";

/** A failed read: what the role lacks when it's a permission, the error otherwise. */
export function QueryError({ title, error }: { title: string; error: unknown }) {
  if (errorCode(error) === Code.PermissionDenied) {
    const m = errorMessage(error);
    // A message that starts with a resource name, such as principals/alex, keeps its case.
    return <EmptyState icon={Lock} title="Your role can't see this" description={`${/^\S+\//.test(m) ? m : sentence(m)}.`} />;
  }
  return <ErrorNote title={title} message={errorMessage(error)} />;
}
