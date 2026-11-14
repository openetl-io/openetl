import { useEffect, useRef } from "react";
import { EditorState, RangeSetBuilder } from "@codemirror/state";
import { Decoration, EditorView, lineNumbers } from "@codemirror/view";
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
  ".cm-line.cm-problem": { backgroundColor: "color-mix(in oklch, var(--status-failed) 14%, transparent)" },
  ".cm-line.cm-note": { backgroundColor: "color-mix(in oklch, var(--signal) 10%, transparent)" },
});

/** Lines to mark, from 1, and how: a problem or a note. */
export type Marks = Map<number, "problem" | "note">;

function marked(doc: EditorState["doc"], marks: Marks) {
  const b = new RangeSetBuilder<Decoration>();
  for (const [n, tone] of [...marks].sort((a, c) => a[0] - c[0])) {
    if (n < 1 || n > doc.lines) continue;
    b.add(doc.line(n).from, doc.line(n).from, Decoration.line({ class: `cm-${tone}` }));
  }
  return EditorView.decorations.of(b.finish());
}

/** A read-only view of YAML, or of SQL as plain text, with line numbers and lines marked. */
export default function CodeView({
  value,
  label,
  language = "yaml",
  marks,
}: {
  value: string;
  label: string;
  language?: "yaml" | "sql";
  marks?: Marks;
}) {
  const host = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const doc = EditorState.create({ doc: value }).doc;
    const view = new EditorView({
      parent: host.current!,
      state: EditorState.create({
        doc,
        extensions: [
          lineNumbers(),
          language === "yaml" ? [yaml(), syntaxHighlighting(highlight)] : [],
          marks?.size ? marked(doc, marks) : [],
          theme,
          EditorView.cspNonce.of(cspNonce),
          EditorState.readOnly.of(true),
          EditorView.editable.of(false),
          EditorView.contentAttributes.of({ "aria-label": label }),
        ],
      }),
    });
    return () => view.destroy();
  }, [value, label, language, marks]);
  return <div ref={host} className="overflow-auto" />;
}
