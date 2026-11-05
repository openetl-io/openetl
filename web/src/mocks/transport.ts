import { Code, ConnectError, createRouterTransport } from "@connectrpc/connect";
import { AccessService } from "@/gen/openetl/api/v1/access_pb";
import { AuditService } from "@/gen/openetl/api/v1/audit_pb";
import { DestinationService, SourceService } from "@/gen/openetl/api/v1/connection_pb";
import { DbtProjectService } from "@/gen/openetl/api/v1/dbt_pb";
import { IncidentService, IncidentState } from "@/gen/openetl/api/v1/incident_pb";
import { SecretService } from "@/gen/openetl/api/v1/secret_pb";
import { WebhookService } from "@/gen/openetl/api/v1/webhook_pb";
import { PipelineService } from "@/gen/openetl/api/v1/pipeline_pb";
import { SyncService } from "@/gen/openetl/api/v1/sync_pb";
import { destinations, pipelines, sources, syncs } from "./fixtures";
import { checkpoints, holds, log, plan, schemas, versions } from "./detail";
import { auditEntries, dataPlanes, dbtProjects, deliveries, incidents, keys, principals as others, runnerTokens, secrets, webhooks } from "./workspace";
import { accessSession, requireSession, user } from "./session";
import { resolve, sendTest, setDisabled, setPaused, startSync } from "./actions";
import { signedOut } from "@/lib/api";

const principals = [user, ...others];

// The API the UI reads, in memory. Methods not here answer Unimplemented.

const latency = () => new Promise((r) => setTimeout(r, 150 + Math.random() * 250));

function find<T extends { name: string }>(list: T[], name: string): T {
  const found = list.find((x) => x.name === name);
  if (!found) throw new ConnectError(`${name} not found`, Code.NotFound);
  return found;
}

export const mockTransport = createRouterTransport(({ service }) => {
  service(PipelineService, {
    listPipelines: async () => (await latency(), { pipelines }),
    getPipeline: async ({ name }) => (await latency(), { pipeline: find(pipelines, name) }),
    getPipelinePlan: async ({ name }) => (await latency(), { plan: plan(name) }),
    listStreamSchemas: async ({ parent }) => (await latency(), { streamSchemas: schemas(parent) }),
    listCheckpoints: async ({ parent }) => (await latency(), { checkpoints: checkpoints(parent) }),
    listHolds: async ({ parent }) => (await latency(), { holds: holds(parent) }),
    listPipelineVersions: async ({ parent }) => (await latency(), { pipelineVersions: versions(find(pipelines, parent)) }),
    pausePipeline: async ({ name }) => (await latency(), setPaused(find(pipelines, name), true)),
    resumePipeline: async ({ name }) => (await latency(), setPaused(find(pipelines, name), false)),
    syncPipeline: async ({ name }) => (await latency(), startSync(find(pipelines, name))),
  });
  service(SourceService, {
    listSources: async () => (await latency(), { sources }),
    getSource: async ({ name }) => (await latency(), { source: find(sources, name) }),
  });
  service(DestinationService, {
    listDestinations: async () => (await latency(), { destinations }),
    getDestination: async ({ name }) => (await latency(), { destination: find(destinations, name) }),
  });
  service(SyncService, {
    listSyncs: async ({ parent, pageSize }) => (await latency(), { syncs: (syncs.get(parent) ?? []).slice(0, pageSize || 50) }),
    getSync: async ({ name }) => (await latency(), { sync: find([...syncs.values()].flat(), name) }),
    readSyncLog: async ({ name }) => (await latency(), { entries: log(find([...syncs.values()].flat(), name)) }),
  });
  service(IncidentService, {
    listIncidents: async ({ parent, state }) => {
      await latency();
      const all = parent.endsWith("/-") ? incidents : incidents.filter((i) => i.name.startsWith(`${parent}/`));
      return { incidents: state === IncidentState.UNSPECIFIED ? all : all.filter((i) => i.state === state) };
    },
    getIncident: async ({ name }) => (await latency(), { incident: find(incidents, name) }),
    resolveIncident: async ({ name, comment }) => (await latency(), resolve(find(incidents, name), comment)),
  });
  service(WebhookService, {
    listWebhooks: async () => (await latency(), { webhooks }),
    getWebhook: async ({ name }) => (await latency(), { webhook: find(webhooks, name) }),
    listWebhookDeliveries: async ({ parent }) => (await latency(), { deliveries: deliveries.get(parent) ?? [] }),
    disableWebhook: async ({ name }) => (await latency(), setDisabled(find(webhooks, name), true)),
    enableWebhook: async ({ name }) => (await latency(), setDisabled(find(webhooks, name), false)),
    testWebhook: async ({ name }) => (await latency(), sendTest(find(webhooks, name))),
  });
  service(SecretService, {
    listSecrets: async () => (await latency(), { secrets }),
  });
  service(DbtProjectService, {
    listDbtProjects: async () => (await latency(), { dbtProjects }),
    getDbtProject: async ({ name }) => (await latency(), { dbtProject: find(dbtProjects, name) }),
  });
  service(AccessService, {
    listPrincipals: async () => (await latency(), { principals }),
    getPrincipal: async ({ name }) => (await latency(), { principal: find(principals, name) }),
    listKeys: async ({ parent }) => (await latency(), { keys: keys.get(parent) ?? [] }),
    listDataPlanes: async () => (await latency(), { dataPlanes }),
    listRunnerTokens: async ({ parent }) => (await latency(), { tokens: runnerTokens.get(parent) ?? [] }),
    getCaller: async () => (await latency(), accessSession.getCaller()),
    createSession: async (req) => (await latency(), accessSession.createSession(req)),
    deleteSession: async () => (await latency(), accessSession.deleteSession()),
    setPassword: async (req) => (await latency(), accessSession.setPassword(req)),
    changePassword: async (req) => (await latency(), accessSession.changePassword(req)),
    revokeSessions: async (req) => (await latency(), accessSession.revokeSessions(req)),
  });
  service(AuditService, {
    listAuditEntries: async ({ actor, resource, method }) => {
      await latency();
      return {
        entries: auditEntries.filter(
          (e) => (!actor || e.actor === actor) && (!resource || e.resources.includes(resource)) && (!method || e.method.endsWith(method)),
        ),
      };
    },
  });
}, { transport: { interceptors: [signedOut] }, router: { interceptors: [requireSession] } });
