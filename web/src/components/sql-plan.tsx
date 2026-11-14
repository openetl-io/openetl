import type { ReactNode } from "react";
import { Badge } from "@/components/ui/badge";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Callout } from "@/components/callout";
import type { SchemaColumn, StatementPlan, ViewPlan } from "@/gen/openetl/api/v1/pipeline_pb";
import { classLabel, position, type SqlMessage } from "@/lib/sql-plan";

/** A place in the SQL, as line:column. */
export function SqlPosition({ line, column }: { line: number; column: number }) {
  return (
    <span className="font-mono text-[0.8125rem] text-muted-foreground tabular-nums" title={`Line ${line}, column ${column}`}>
      {line}:{column}
    </span>
  );
}

/** The plan's problems and notes about the SQL, each with its place. */
export function SqlMessages({ messages }: { messages: SqlMessage[] }) {
  if (!messages.length) return null;
  return (
    <div className="grid gap-1.5">
      {messages.map((m, i) => (
        <Callout key={i} tone={m.tone}>
          {m.line && (
            <>
              <span className="font-mono text-[0.8125rem] tabular-nums" title={`Line ${m.line}, column ${m.column}`}>
                {position(m)}
              </span>{" "}
            </>
          )}
          {m.message}
        </Callout>
      ))}
    </div>
  );
}

/** A statement's facts, which wrap rather than truncate: names can be long. */
function Facts({ children }: { children: ReactNode }) {
  return <dl className="grid gap-x-6 gap-y-3 sm:grid-cols-3">{children}</dl>;
}

function Fact({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="grid min-w-0 content-start gap-0.5">
      <dt className="text-[0.8125rem] text-muted-foreground">{label}</dt>
      <dd className="min-w-0 break-words">{children}</dd>
    </div>
  );
}

function Names({ names, none }: { names: string[]; none: string }) {
  if (!names.length) return <span className="text-muted-foreground">{none}</span>;
  return <span className="font-mono text-[0.8125rem]">{names.join(", ")}</span>;
}

/** An output's or a view's columns. */
export function SqlColumns({ columns, label }: { columns: SchemaColumn[]; label: string }) {
  return (
    <div className="overflow-x-auto rounded-md border contain-inline-size">
      <Table aria-label={label}>
        <TableHeader>
          <TableRow>
            <TableHead>Column</TableHead>
            <TableHead>Type</TableHead>
            <TableHead>Nulls</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {columns.map((c) => (
            <TableRow key={c.name}>
              <TableCell className="font-mono text-[0.8125rem]">{c.name}</TableCell>
              <TableCell className="font-mono text-[0.8125rem]">{c.type}</TableCell>
              <TableCell className="text-muted-foreground">{c.nullable ? "Allowed" : "Not null"}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

/** One INSERT INTO … SELECT as planned: its output, what it reads, its key and its columns. */
export function SqlStatement({ statement: s, table, messages }: { statement: StatementPlan; table?: string; messages: SqlMessage[] }) {
  return (
    <Card className="gap-4 py-4">
      <CardHeader className="px-4">
        <CardTitle className="flex flex-wrap items-baseline gap-x-2 gap-y-1 font-normal">
          <span className="font-mono text-[0.8125rem] text-muted-foreground">INSERT INTO</span>
          <span className="font-mono font-semibold break-all">{s.output}</span>
          {table && <span className="font-mono text-[0.8125rem] text-muted-foreground">→ {table}</span>}
        </CardTitle>
        <CardDescription>
          <SqlPosition line={s.line} column={s.column} />
        </CardDescription>
        <CardAction className="flex flex-wrap justify-end gap-1.5">
          <Badge variant="outline">{classLabel[s.class] ?? s.class}</Badge>
          {s.replaces && <Badge variant="secondary">Replaces stream</Badge>}
        </CardAction>
      </CardHeader>
      <CardContent className="grid gap-4 px-4">
        <Facts>
          <Fact label="Primary key">
            <Names names={s.primaryKey} none="None, so append only" />
          </Fact>
          <Fact label="Reads">
            <Names names={s.inputs} none="—" />
            {s.views.length > 0 && (
              <span className="text-muted-foreground">
                {" "}through <Names names={s.views} none="" />
              </span>
            )}
          </Fact>
          <Fact label="Exchange">
            <Names names={s.exchange} none="None" />
          </Fact>
        </Facts>
        <SqlMessages messages={messages} />
        <SqlColumns columns={s.columns} label={`The columns of ${s.output}`} />
      </CardContent>
    </Card>
  );
}

/** A CREATE VIEW: its name, what it reads, and its columns. */
export function SqlView({ view: v, messages }: { view: ViewPlan; messages: SqlMessage[] }) {
  return (
    <Card className="gap-4 py-4">
      <CardHeader className="px-4">
        <CardTitle className="flex flex-wrap items-baseline gap-x-2 font-normal">
          <span className="font-mono text-[0.8125rem] text-muted-foreground">CREATE VIEW</span>
          <span className="font-mono font-semibold break-all">{v.name}</span>
        </CardTitle>
        <CardDescription>
          <SqlPosition line={v.line} column={v.column} />
        </CardDescription>
        <CardAction>
          <Badge variant="outline">View</Badge>
        </CardAction>
      </CardHeader>
      <CardContent className="grid gap-4 px-4">
        <Facts>
          <Fact label="Primary key">
            <Names names={v.primaryKey} none="None" />
          </Fact>
          <Fact label="Reads">
            <Names names={v.inputs} none="—" />
          </Fact>
        </Facts>
        <SqlMessages messages={messages} />
        <SqlColumns columns={v.columns} label={`The columns of view ${v.name}`} />
      </CardContent>
    </Card>
  );
}

/** A list of the source's streams under a label, such as those that pass through. */
export function SqlStreams({ label, streams, children }: { label: string; streams: string[]; children?: ReactNode }) {
  if (!streams.length) return null;
  return (
    <div className="grid gap-1">
      <h3 className="text-[0.8125rem] text-muted-foreground">{label}</h3>
      <p className="font-mono text-[0.8125rem] break-all">{streams.join(", ")}</p>
      {children && <p className="text-[0.8125rem] text-muted-foreground">{children}</p>}
    </div>
  );
}
