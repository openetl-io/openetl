import { useMemo } from "react";
import { useQuery } from "@connectrpc/connect-query";
import { getRouteApi } from "@tanstack/react-router";
import { Plug, TriangleAlert } from "lucide-react";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { PackageCard, PackageCardSkeleton, PackageSheet, SourcesTable } from "@/components/catalog";
import { CommandLine } from "@/components/command-line";
import { FacetedFilter } from "@/components/data-table/faceted-filter";
import { Toolbar } from "@/components/data-table/toolbar";
import { EmptyState } from "@/components/empty-state";
import { PageHeader } from "@/components/page-header";
import { QueryError } from "@/components/query-error";
import { ConnectorService, type CatalogPackage } from "@/gen/openetl/api/v1/connector_pb";
import { failingSources, packageText, roleLabel, sortPackages, sourceId } from "@/lib/catalog";
import { useListFilter } from "@/lib/list-filter";

const route = getRouteApi("/_app/workspaces/$workspace/connectors");

const facets = { role: (p: CatalogPackage) => p.roles, source: (p: CatalogPackage) => sourceId(p.source) };

/** A ConnectorSource document, to start from. */
const sourceExample = "openetl connector apply -f connector-source.yaml";

function Catalog() {
  const search = route.useSearch();
  const navigate = route.useNavigate();
  const catalog = useQuery(ConnectorService.method.listCatalog, {}, { refetchInterval: 60_000 });
  const sources = useQuery(ConnectorService.method.listConnectorSources, {}, { refetchInterval: 60_000 });
  const all = useMemo(() => sortPackages(catalog.data?.packages ?? []), [catalog.data]);
  const list = useListFilter({ rows: all, search, facets, text: packageText });
  const setSearch = (s: Partial<typeof search>) => navigate({ search: (prev) => ({ ...prev, ...s }), replace: true });
  const reset = () => setSearch({ q: undefined, role: undefined, source: undefined });
  const failing = failingSources(sources.data?.sources ?? []);

  if (catalog.error) return <QueryError title="Couldn't list the catalog" error={catalog.error} />;
  if (sources.data?.sources.length === 0) {
    return (
      <EmptyState
        icon={Plug}
        title="No sources of connectors are connected"
        description="A ConnectorSource document connects an OCI registry namespace or a GitHub repository, and the catalog lists the connector packages it holds."
        command={sourceExample}
      />
    );
  }
  if (catalog.data && all.length === 0) {
    return (
      <EmptyState
        icon={Plug}
        title="The sources list no connectors yet"
        description="Each source is listed every 15 minutes. Listing them now takes a command."
        command="openetl connector catalog --refresh"
      />
    );
  }
  return (
    <div className="grid gap-4">
      {failing.length > 0 && (
        <Alert className="[&>svg]:text-degraded">
          <TriangleAlert />
          <AlertTitle>
            {failing.length === 1 ? `The ${sourceId(failing[0]!.name)} source failed to list` : `${failing.length} sources failed to list`}
          </AlertTitle>
          <AlertDescription>
            <p>
              Its connectors are from its last listing.{" "}
              <Button variant="link" className="h-auto p-0" onClick={() => setSearch({ tab: "sources" })}>
                See why
              </Button>
            </p>
          </AlertDescription>
        </Alert>
      )}
      <Toolbar search={search.q ?? ""} onSearch={(v) => setSearch({ q: v || undefined })} placeholder="Search connectors" filtered={list.filtered} onReset={reset}>
        <FacetedFilter
          title="Role"
          options={list.values("role", ["source", "destination"]).map((o) => ({ ...o, label: roleLabel(o.value) }))}
          selected={search.role ?? []}
          onChange={(v) => setSearch({ role: v.length ? v : undefined })}
        />
        <FacetedFilter
          title="Source"
          options={list.values("source").map((o) => ({ ...o, label: o.value }))}
          selected={search.source ?? []}
          onChange={(v) => setSearch({ source: v.length ? v : undefined })}
        />
      </Toolbar>
      {catalog.isPending ? (
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4" aria-busy>
          {Array.from({ length: 6 }, (_, i) => (
            <PackageCardSkeleton key={i} />
          ))}
        </div>
      ) : list.visible.length === 0 ? (
        <p className="flex items-center gap-2 rounded-lg border border-dashed p-6 text-sm text-muted-foreground">
          No connectors match.
          <Button variant="link" size="sm" className="h-auto p-0" onClick={reset}>
            Reset the filters
          </Button>
        </p>
      ) : (
        <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3 2xl:grid-cols-4">
          {list.visible.map((p) => (
            <PackageCard key={p.name} pkg={p} onOpen={() => setSearch({ package: p.name })} />
          ))}
        </div>
      )}
      <PackageSheet pkg={all.find((p) => p.name === search.package)} onClose={() => setSearch({ package: undefined })} />
    </div>
  );
}

function Sources() {
  const sources = useQuery(ConnectorService.method.listConnectorSources, {}, { refetchInterval: 60_000 });
  if (sources.error) return <QueryError title="Couldn't list the sources" error={sources.error} />;
  return (
    <div className="grid gap-4">
      <p className="max-w-3xl text-sm text-muted-foreground">
        Each source, an OCI registry namespace or a GitHub repository, is listed every 15 minutes, and a source that fails keeps its last listing. A
        ConnectorSource document connects one.
      </p>
      <SourcesTable sources={sources.data?.sources ?? []} loading={sources.isPending} />
      <div className="grid max-w-xl gap-2">
        <CommandLine command="openetl connector catalog --refresh" />
        <CommandLine command={sourceExample} />
      </div>
    </div>
  );
}

export function ConnectorsPage() {
  const search = route.useSearch();
  const navigate = route.useNavigate();
  const catalog = useQuery(ConnectorService.method.listCatalog, {});
  return (
    <div className="grid gap-4">
      <PageHeader title="Connectors" count={catalog.data?.packages.length} />
      <Tabs value={search.tab ?? "catalog"} onValueChange={(t) => navigate({ search: { tab: t === "sources" ? "sources" : undefined }, replace: true })}>
        <TabsList variant="line">
          <TabsTrigger value="catalog">Catalog</TabsTrigger>
          <TabsTrigger value="sources">Sources</TabsTrigger>
        </TabsList>
        <TabsContent value="catalog" className="pt-4">
          <Catalog />
        </TabsContent>
        <TabsContent value="sources" className="pt-4">
          <Sources />
        </TabsContent>
      </Tabs>
    </div>
  );
}
