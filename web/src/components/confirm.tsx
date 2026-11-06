import { useState, type ReactNode } from "react";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog";

/** Asks before an action, and stays open until the action is done. */
export function Confirm({
  title,
  description,
  body,
  confirm,
  destructive,
  pending,
  onConfirm,
  onOpenChange,
  children,
}: {
  title: string;
  description: ReactNode;
  /** Fields between the description and the buttons. */
  body?: ReactNode;
  /** The confirm button's label. */
  confirm: string;
  destructive?: boolean;
  pending: boolean;
  /** Takes the action; close() once it's done. */
  onConfirm: (close: () => void) => void;
  onOpenChange?: (open: boolean) => void;
  /** The trigger, a Button. */
  children: ReactNode;
}) {
  const [open, setOpen] = useState(false);
  const change = (o: boolean) => {
    setOpen(o);
    onOpenChange?.(o);
  };
  return (
    <AlertDialog open={open} onOpenChange={change}>
      <AlertDialogTrigger asChild>{children}</AlertDialogTrigger>
      <AlertDialogContent>
        <AlertDialogHeader>
          <AlertDialogTitle>{title}</AlertDialogTitle>
          <AlertDialogDescription>{description}</AlertDialogDescription>
        </AlertDialogHeader>
        {body}
        <AlertDialogFooter>
          <AlertDialogCancel>Cancel</AlertDialogCancel>
          <AlertDialogAction
            variant={destructive ? "destructive" : "default"}
            disabled={pending}
            onClick={(e) => {
              e.preventDefault();
              onConfirm(() => change(false));
            }}
          >
            {confirm}
          </AlertDialogAction>
        </AlertDialogFooter>
      </AlertDialogContent>
    </AlertDialog>
  );
}
