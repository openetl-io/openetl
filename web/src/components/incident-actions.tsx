import { useState } from "react";
import { CircleCheck } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Field, FieldDescription, FieldLabel } from "@/components/ui/field";
import { Textarea } from "@/components/ui/textarea";
import { IncidentService, IncidentState, type Incident } from "@/gen/openetl/api/v1/incident_pb";
import { PipelineService } from "@/gen/openetl/api/v1/pipeline_pb";
import { useAction } from "@/lib/action";
import { useCan } from "@/lib/caller";
import { Confirm } from "./confirm";

/** Resolving an open incident by hand, for a role that may. */
export function IncidentActions({ workspace, incident }: { workspace: string; incident: Incident }) {
  const can = useCan(workspace);
  const [comment, setComment] = useState("");
  // The pipeline list counts open incidents.
  const resolve = useAction(IncidentService.method.resolveIncident, { done: "Resolved", failed: "Couldn't resolve", refresh: [IncidentService, PipelineService] });
  if (incident.state !== IncidentState.OPEN || !can("incidents.resolve")) return null;

  return (
    <Confirm
      title="Resolve this incident?"
      description="Resolve it once it's fixed in a way openetl can't see. If it happens again, a new incident opens."
      body={
        <Field>
          <FieldLabel htmlFor="resolve-comment">Comment</FieldLabel>
          <Textarea id="resolve-comment" maxLength={1000} value={comment} onChange={(e) => setComment(e.target.value)} />
          <FieldDescription>Optional. It's kept with the resolution.</FieldDescription>
        </Field>
      }
      confirm={resolve.isPending ? "Resolving…" : "Resolve"}
      pending={resolve.isPending}
      onConfirm={(close) => resolve.mutate({ name: incident.name, comment }, { onSuccess: close })}
      onOpenChange={(o) => o || setComment("")}
    >
      <Button size="sm" variant="outline">
        <CircleCheck aria-hidden />
        Resolve
      </Button>
    </Confirm>
  );
}
