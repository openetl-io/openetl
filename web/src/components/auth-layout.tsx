import type { ReactNode } from "react";
import { Checkbox } from "@/components/ui/checkbox";
import { Field, FieldLabel } from "@/components/ui/field";
import { Logo } from "./logo";

/** The pages outside the app: the logo, a title, and a narrow form. */
export function AuthLayout({ title, description, children }: { title: string; description?: ReactNode; children: ReactNode }) {
  return (
    <main className="grid min-h-dvh place-items-center px-4 py-10">
      <div className="grid w-full max-w-sm gap-6">
        <div className="grid justify-items-start gap-4">
          <Logo motion="intro" className="size-12" />
          <div className="grid gap-1">
            <h1 className="text-xl font-semibold tracking-tight">{title}</h1>
            {description && <p className="text-muted-foreground">{description}</p>}
          </div>
        </div>
        {children}
      </div>
    </main>
  );
}

export function RememberMe({ checked, onChange }: { checked: boolean; onChange: (v: boolean) => void }) {
  return (
    <Field orientation="horizontal">
      <Checkbox id="remember" checked={checked} onCheckedChange={(v) => onChange(v === true)} />
      <FieldLabel htmlFor="remember" className="font-normal text-muted-foreground">
        Keep me signed in on this device
      </FieldLabel>
    </Field>
  );
}
