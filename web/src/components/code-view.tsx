import { useEffect, useRef } from "react";
import { EditorState } from "@codemirror/state";
import { EditorView, lineNumbers } from "@codemirror/view";
import { yaml } from "@codemirror/lang-yaml";
import { syntaxHighlighting, HighlightStyle } from "@codemirror/language";
import { tags } from "@lezer/highlight";
import { cspNonce } from "@/lib/csp";

const highlight = HighlightStyle.define([
  { tag: [tags.propertyName, tags.definition(tags.propertyName)], color: "var(--foreground)" },
  { tag: [tags.string, tags.special(tags.string)], color: "var(--signal)" },
  { tag: [tags.number, tags.bool, tags.null], color: "var(--status-degraded)" },
  { tag: [tags.comment, tags.meta, tags.punctuation], color: "var(--muted-foreground)" },
]);

const theme = EditorView.theme({
  "&": { fontSize: "0.8125rem", backgroundColor: "transparent", color: "var(--foreground)" },
  ".cm-content": { fontFamily: "var(--font-mono)", padding: "12px 0" },
  ".cm-gutters": { backgroundColor: "transparent", border: "none", color: "var(--muted-foreground)" },
  ".cm-lineNumbers .cm-gutterElement": { padding: "0 12px 0 16px" },
  "&.cm-focused": { outline: "none" },
  ".cm-selectionBackground, &.cm-focused .cm-selectionBackground": { backgroundColor: "var(--signal-soft) !important" },
});

/** A read-only YAML view. */
export default function CodeView({ value, label }: { value: string; label: string }) {
  const host = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const view = new EditorView({
      parent: host.current!,
      state: EditorState.create({
        doc: value,
        extensions: [
          lineNumbers(),
          yaml(),
          syntaxHighlighting(highlight),
          theme,
          EditorView.cspNonce.of(cspNonce),
          EditorState.readOnly.of(true),
          EditorView.editable.of(false),
          EditorView.contentAttributes.of({ "aria-label": label }),
        ],
      }),
    });
    return () => view.destroy();
  }, [value, label]);
  return <div ref={host} className="overflow-auto" />;
}
