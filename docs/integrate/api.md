---
title: Connect to the API
description: Choose an API transport and authenticate an integration with a service account.
---

Use the API to connect your application to OpenETL. This guide assumes that you know HTTP and API authentication.

## The API

Everything the CLI and the console do goes through OpenETL's API, served by `openetld server`. The same API is served four ways:

- **REST**, with JSON, under `/v1`, such as `GET /v1/workspaces/default/pipelines`. The server serves its OpenAPI description at `/openapi.yaml`.
- **Connect, gRPC and gRPC-Web**, from the protos in [`proto/openetl/api/v1`](../../proto/openetl/api/v1), such as `openetl.api.v1.PipelineService/ListPipelines`.

Send an API key in the `Authorization: Bearer KEY` header. Give each integration a separate service account with the minimum required role:

- `viewer` reads resources.
- `operator` can also start syncs and pause or resume pipelines.
- `editor` can also apply and delete resources.

```sh
openetl principal create reporting
openetl principal grant reporting viewer
openetl key create --out reporting.key reporting
curl -H "Authorization: Bearer $(cat reporting.key)" http://localhost:8080/v1/workspaces/default/pipelines
```

Responses use protobuf JSON. Enum values use their full names, such as `PIPELINE_MODE_SCHEDULED`. Durations use seconds, such as `3600s`.

Use `ApplyService` (`POST /v1/workspaces/default:apply`) to create or update sources, destinations, pipelines and other resources. It accepts the same documents sent by `openetl apply`.

For check and preflight response fields, see [Check results in the API](./api-checks.md).
