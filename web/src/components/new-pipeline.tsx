import { Plus } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle, DialogTrigger } from "@/components/ui/dialog";
import { CommandLine } from "./command-line";

/** Until the UI can create pipelines, how to create one with the CLI. */
export function NewPipelineButton() {
  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button size="sm">
          <Plus aria-hidden />
          New pipeline
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>New pipeline</DialogTitle>
          <DialogDescription>
            A pipeline is a YAML document that the CLI applies. Check what it would do with a plan, then apply it.
          </DialogDescription>
        </DialogHeader>
        <div className="grid gap-2">
          <CommandLine command="openetl plan -f pipeline.yaml" />
          <CommandLine command="openetl apply -f pipeline.yaml" />
        </div>
      </DialogContent>
    </Dialog>
  );
}
