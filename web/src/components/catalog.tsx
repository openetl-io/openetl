import { useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import type { ColumnDef } from "@tanstack/react-table";
import { BadgeCheck, Check, CircleCheck, ExternalLink, KeyRound, PackagePlus, TriangleAlert } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle } from "@/components/ui/sheet";
import { Skeleton } from "@/components/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { DataTable } from "@/components/data-table/data-table";
import { SortHeader } from "@/components/data-table/sort-header";
import { useDataTable } from "@/components/data-table/use-data-table";
import { CommandLine } from "@/components/command-line";
import { ConnectorLogo } from "@/components/connector";
import { DescriptionItem, DescriptionList, Section } from "@/components/detail-header";
import { ErrorNote } from "@/components/empty";
import { QueryError } from "@/components/query-error";
import { RelativeTime } from "@/components/relative-time";
import { type CatalogPackage, type CatalogRole, ConnectorService, type ConnectorSource } from "@/gen/openetl/api/v1/connector_pb";
import { appliedState, applyCommand, manualLabel, roleLabel, sourceId } from "@/lib/catalog";
import { dateOf, relative } from "@/lib/format";
import { cn } from "@/lib/utils";

/** Official, not official, manually installed, and which version the org has applied. */
export function PackageBadges({ pkg, className }: { pkg: CatalogPackage; className?: string }) {
  const applied = appliedState(pkg);
  if (!pkg.official && !pkg.impostor && !pkg.manual && applied === "none") return null;
  return (
    <span className={cn("inline-flex flex-wrap items-center gap-1", className)}>
      {pkg.official && (
        <Badge variant="secondary">
          <BadgeCheck className="text-running" aria-hidden />
          Official
        </Badge>
      )}
      {pkg.impostor && (
        <Badge variant="outline" className="border-failed/40 text-failed">
          <TriangleAlert aria-hidden />
          Not official
        </Badge>
      )}
      {pkg.manual && (
        <Badge variant="outline">
          <PackagePlus aria-hidden />
          {manualLabel}
        </Badge>
      )}
      {applied !== "none" && (
        <Badge variant="outline" className={cn(applied === "current" && "border-healthy/40")}>
          {applied === "current" ? <CircleCheck className="text-healthy" aria-hidden /> : <Check aria-hidden />}
          Applied {pkg.appliedVersion}
        </Badge>
      )}
    </span>
  );
}

const roleOrder = (r: string) => (r === "source" ? 0 : r === "destination" ? 1 : 2);

export function RoleBadges({ roles }: { roles: string[] }) {
  return (
    <span className="inline-flex flex-wrap gap-1">
      {[...roles].sort((a, b) => roleOrder(a) - roleOrder(b)).map((r) => (
        <Badge key={r} variant="outline" className="font-normal">
          {roleLabel(r)}
        </Badge>
      ))}
    </span>
  );
}

function Tags({ tags, max }: { tags: string[]; max?: number }) {
  const shown = max === undefined ? tags : tags.slice(0, max);
  return (
    <span className="inline-flex flex-wrap gap-1">
      {shown.map((t) => (
        <Badge key={t} variant="secondary" className="font-normal">
          {t}
        </Badge>
      ))}
      {tags.length > shown.length && <span className="text-xs text-muted-foreground">+{tags.length - shown.length}</span>}
    </span>
  );
}

/** A package in the catalog's grid: the whole card opens it. */
export function PackageCard({ pkg, onOpen }: { pkg: CatalogPackage; onOpen: () => void }) {
  return (
    <Card className="relative gap-3 py-4 transition-colors hover:bg-accent/50 has-focus-visible:ring-[3px] has-focus-visible:ring-ring/50">
      <CardHeader className="flex items-start gap-3 px-4">
        <ConnectorLogo logo={pkg.logo} connector={pkg.package} size="lg" />
        <div className="grid min-w-0 flex-1 gap-1">
          <CardTitle className="truncate leading-tight">
            <button type="button" onClick={onOpen} className="text-left outline-none after:absolute after:inset-0 after:rounded-xl">
              {pkg.title}
            </button>
          </CardTitle>
          <CardDescription className="truncate font-mono text-xs">{pkg.package}</CardDescription>
        </div>
      </CardHeader>
      <CardContent className="grid gap-2 px-4">
        <PackageBadges pkg={pkg} />
        <p className={cn("line-clamp-2 text-sm", pkg.description ? "text-muted-foreground" : "text-muted-foreground/60 italic")}>
          {pkg.description || "No description"}
        </p>
        {pkg.tags.length > 0 && <Tags tags={pkg.tags} max={3} />}
      </CardContent>
      <CardFooter className="mt-auto flex-wrap justify-between gap-x-3 gap-y-2 px-4 text-xs text-muted-foreground">
        <span className="inline-flex min-w-0 items-center gap-1.5">
          <span className="font-mono text-foreground">{pkg.version}</span>
          {pkg.author && (
            <>
              <span aria-hidden>·</span>
              <span className="truncate">{pkg.author}</span>
            </>
          )}
        </span>
        <RoleBadges roles={pkg.roles} />
      </CardFooter>
    </Card>
  );
}

export function PackageCardSkeleton() {
  return (
    <Card className="gap-3 py-4">
      <CardHeader className="flex items-center gap-3 px-4">
        <Skeleton className="size-10 rounded-md" />
        <div className="grid flex-1 gap-1.5">
          <Skeleton className="h-4 w-32" />
          <Skeleton className="h-3 w-24" />
        </div>
      </CardHeader>
      <CardContent className="grid gap-2 px-4">
        <Skeleton className="h-4 w-full" />
        <Skeleton className="h-4 w-2/3" />
      </CardContent>
    </Card>
  );
}

function OutLink({ href }: { href: string }) {
  return (
    <a href={href} target="_blank" rel="noreferrer" className="inline-flex max-w-full items-center gap-1 hover:underline">
      <span className="truncate">{href.replace(/^https:\/\//, "")}</span>
      <ExternalLink className="size-3.5 shrink-0 text-muted-foreground" aria-hidden />
    </a>
  );
}

/** A role's config fields, from its JSON Schema: required ones first. */
export function ConfigFields({ role }: { role: CatalogRole }) {
  if (role.config.length === 0) return <p className="text-sm text-muted-foreground">Its config takes no fields.</p>;
  return (
    <div className="overflow-hidden rounded-lg border contain-inline-size">
      <Table>
        <TableHeader className="bg-muted/60">
          <TableRow className="hover:bg-transparent">
            <TableHead>Field</TableHead>
            <TableHead className="hidden sm:table-cell">Type</TableHead>
            <TableHead>Required</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {role.config.map((f) => (
            <TableRow key={f.name}>
              <TableCell className="align-top whitespace-normal">
                <span className="inline-flex items-center gap-1.5 font-mono text-[0.8125rem]">
                  {f.name}
                  {f.secret && (
                    <Badge variant="outline" className="font-sans font-normal">
                      <KeyRound aria-hidden />
                      Secret
                    </Badge>
                  )}
                </span>
                {(f.title || f.description) && (
                  <span className="mt-0.5 block text-xs text-muted-foreground">
                    {f.title && f.title !== f.name && <span className="text-foreground">{f.title}. </span>}
                    {f.description}
                  </span>
                )}
                <span className="mt-0.5 block font-mono text-xs text-muted-foreground sm:hidden">{f.type || "any"}</span>
              </TableCell>
              <TableCell className="hidden align-top font-mono text-[0.8125rem] text-muted-foreground sm:table-cell">{f.type || "any"}</TableCell>
              <TableCell className="align-top">{f.required ? "Yes" : <span className="text-muted-foreground">No</span>}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

/** Its roles' config fields, which the API reads from the descriptor the first time someone opens the package. */
function PackageRoles({ name }: { name: string }) {
  const q = useQuery(ConnectorService.method.getCatalogPackage, { name });
  if (q.error) return <QueryError title="Couldn't read its descriptor" error={q.error} />;
  if (!q.data) {
    return (
      <div className="grid gap-2" aria-busy>
        <Skeleton className="h-4 w-40" />
        <Skeleton className="h-28 w-full" />
      </div>
    );
  }
  return (
    <div className="grid grid-cols-1 gap-6">
      {[...q.data.roles].sort((a, b) => roleOrder(a.role) - roleOrder(b.role)).map((r) => (
        <Section key={r.role} title={`${roleLabel(r.role)} config`}>
          <ConfigFields role={r} />
        </Section>
      ))}
    </div>
  );
}

/** A package opened from the grid: what its publisher says, its config, and how to apply it. */
export function PackageSheet({ pkg, onClose }: { pkg?: CatalogPackage; onClose: () => void }) {
  return (
    <Sheet open={!!pkg} onOpenChange={(o) => !o && onClose()}>
      <SheetContent className="w-full overflow-y-auto sm:max-w-xl">
        {pkg && (
          <>
            <SheetHeader className="flex-row items-center gap-3">
              <ConnectorLogo logo={pkg.logo} connector={pkg.package} size="lg" />
              <div className="grid min-w-0 gap-1">
                <SheetTitle className="truncate">{pkg.title}</SheetTitle>
                <SheetDescription className="truncate font-mono text-xs">
                  {pkg.package} {pkg.version}
                </SheetDescription>
              </div>
            </SheetHeader>
            <div className="grid grid-cols-1 gap-6 px-4 pb-6">
              <PackageBadges pkg={pkg} />
              {pkg.description && <p className="text-sm">{pkg.description}</p>}
              {pkg.impostor && (
                <Alert variant="destructive">
                  <TriangleAlert />
                  <AlertTitle>Not an official connector</AlertTitle>
                  <AlertDescription>
                    Its name is under openetl/, which only the official connectors use, and the {sourceId(pkg.source)} source lists it, so applying it is
                    refused.
                  </AlertDescription>
                </Alert>
              )}
              <DescriptionList className="grid-cols-1 sm:grid-cols-2 lg:grid">
                <DescriptionItem label="Version">
                  <span className="font-mono text-[0.8125rem]">{pkg.version}</span>
                </DescriptionItem>
                <DescriptionItem label="Applied">
                  {pkg.appliedVersion ? (
                    <span className="font-mono text-[0.8125rem]">{pkg.appliedVersion}</span>
                  ) : (
                    <span className="text-muted-foreground">Not applied</span>
                  )}
                </DescriptionItem>
                <DescriptionItem label="Author">{pkg.author || <span className="text-muted-foreground">—</span>}</DescriptionItem>
                <DescriptionItem label="License">{pkg.license || <span className="text-muted-foreground">—</span>}</DescriptionItem>
                <DescriptionItem label="Roles">
                  <RoleBadges roles={pkg.roles} />
                </DescriptionItem>
                <DescriptionItem label="Listed by">
                  {pkg.manual ? <span className="text-muted-foreground">No connected source</span> : sourceId(pkg.source)}
                </DescriptionItem>
                {pkg.repository && (
                  <DescriptionItem label="Repository" className="sm:col-span-2">
                    <OutLink href={pkg.repository} />
                  </DescriptionItem>
                )}
                {pkg.docs && (
                  <DescriptionItem label="Docs" className="sm:col-span-2">
                    <OutLink href={pkg.docs} />
                  </DescriptionItem>
                )}
              </DescriptionList>
              {pkg.tags.length > 0 && <Tags tags={pkg.tags} />}
              {!pkg.impostor && (
                <Section title="Apply">
                  <p className="text-sm text-muted-foreground">
                    {appliedState(pkg) === "current"
                      ? "The org has applied this version. Sources and destinations name it as "
                      : "Applying it makes the version available to sources and destinations, which name it as "}
                    <code className="font-mono text-[0.8125rem] text-foreground">{pkg.package.replace(/^openetl\//, "")}</code>.
                  </p>
                  {pkg.reference && <CommandLine command={applyCommand(pkg)} className="min-w-0" />}
                  {pkg.manual && (
                    <p className="text-sm text-muted-foreground">
                      It was applied from a file or by a reference, and no connected source lists it, so OpenETL can't say whether a newer version exists.
                    </p>
                  )}
                </Section>
              )}
              <PackageRoles name={pkg.name} />
            </div>
          </>
        )}
      </SheetContent>
    </Sheet>
  );
}

function Listed({ source }: { source: ConnectorSource }) {
  const t = dateOf(source.refreshTime);
  return t ? <RelativeTime date={t} /> : <span className="text-muted-foreground">Never</span>;
}

/** The connected sources of connectors, with the error each last failed to list with. */
export function SourcesTable({ sources, loading }: { sources: ConnectorSource[]; loading: boolean }) {
  const columns = useMemo<ColumnDef<ConnectorSource>[]>(
    () => [
      {
        id: "name",
        accessorFn: (s) => sourceId(s.name),
        header: ({ column }) => <SortHeader column={column} title="Source" />,
        cell: ({ row }) => (
          <span className="inline-flex items-center gap-2">
            <span className="font-medium">{sourceId(row.original.name)}</span>
            {row.original.official && (
              <Badge variant="secondary">
                <BadgeCheck className="text-running" aria-hidden />
                Official
              </Badge>
            )}
          </span>
        ),
      },
      {
        id: "url",
        header: "URL",
        cell: ({ row }) => <span className="block max-w-96 truncate font-mono text-[0.8125rem] text-muted-foreground">{row.original.spec?.url}</span>,
        meta: { className: "hidden md:table-cell" },
      },
      {
        id: "packages",
        accessorFn: (s) => s.packages,
        header: ({ column }) => <SortHeader column={column} title="Packages" />,
        cell: ({ row }) => <span className="tabular-nums">{row.original.packages}</span>,
        meta: { align: "right" },
      },
      {
        id: "listed",
        accessorFn: (s) => dateOf(s.refreshTime)?.getTime() ?? 0,
        header: ({ column }) => <SortHeader column={column} title="Listed" />,
        cell: ({ row }) =>
          row.original.error ? (
            <span className="inline-flex items-center gap-1.5 whitespace-nowrap text-failed">
              <TriangleAlert className="size-4 shrink-0" aria-hidden />
              <span className="sr-only">Failing; last listed</span>
              <Listed source={row.original} />
            </span>
          ) : (
            <Listed source={row.original} />
          ),
        meta: { className: "hidden sm:table-cell" },
      },
    ],
    [],
  );
  const table = useDataTable({ data: sources, columns, sort: [{ id: "name", desc: false }], paged: false });
  return (
    <div className="grid gap-4">
      {sources
        .filter((s) => s.error)
        .map((s) => {
          const t = dateOf(s.errorTime);
          return (
            <ErrorNote
              key={s.name}
              title={`${sourceId(s.name)} failed to list${t ? ` ${relative(t)}` : ""}; it keeps its last listing`}
              message={s.error}
            />
          );
        })}
      <DataTable table={table} loading={loading} empty="No sources are connected." />
    </div>
  );
}

