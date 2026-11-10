# OpenETL

OpenETL copies tables, changes and events from databases and event streams into warehouses, data lakes and other streams. Pipelines run on a schedule or continuously. OpenETL checks each stream's delivery guarantee and can commit each change exactly once when the connectors support it.

You run OpenETL on your own infrastructure. It is open source under Apache 2.0.

> [!NOTE]
> OpenETL is pre-alpha. There are no releases. The pipeline format and API can change.

## What you can do with it

- **Connect your systems** with source and destination connectors. Use an official connector, one maintained by the community, or one your organization builds for its own systems.
- **Choose how often:** the same pipeline runs every hour, every day, or continuously.
- **Set delivery requirements:** OpenETL checks whether each stream can receive duplicate changes. It refuses to run a pipeline that cannot meet its required guarantee.
- **Shape the data** with dbt after each sync, or on your own Spark and Flink clusters.
- **Monitor pipelines** in the console. Check data freshness, failures and recovery steps. Export metrics and traces through OpenTelemetry.
- **Keep pipelines in git** as YAML, and review a plan of each change before you apply it.

## Get started

Until the first release, you run OpenETL from source. You need Git, [Go](https://go.dev/dl/) 1.26, [Rust](https://rustup.rs), [Node](https://nodejs.org) 22 with [pnpm](https://pnpm.io/installation), and [Docker](https://docs.docker.com/get-started/get-docker/).

```sh
git clone https://github.com/openetl-io/openetl
git clone https://github.com/openetl-io/openetl-connectors
cd openetl
make dev DEPS=postgres
```

The first build takes a few minutes. When the log shows `API serving`, open the console at [localhost:8080](http://localhost:8080).

This example uses the official [Postgres connector](https://github.com/openetl-io/openetl-connectors/blob/main/postgres/README.md) built by `make dev`. Open a second terminal in the same directory. Create a pipeline that copies generated rows into Postgres:

```sh
go build -o bin/ ./cmd/openetl && export PATH="$PWD/bin:$PATH"
echo postgres://openetl:openetl@localhost:5432/openetl | openetl secret set local-postgres-dsn
openetl apply -f examples/faker-to-postgres.yaml
```

The first sync starts immediately. Open **Pipelines** in the console to check its result. The [quickstart](docs/get-started/quickstart.md) explains each step, verifies the copied data and shows how to stop the services.

## Documentation

| If you want to | Start with |
|---|---|
| Learn what OpenETL is, in plain words | [Overview](docs/get-started/overview.md) |
| Run OpenETL for your team | [Self-host](docs/self-host/index.md) |
| Build pipelines | [Pipelines](docs/pipelines/index.md) |
| Check on pipelines and the data they deliver | [Use the console](docs/use/index.md) |
| Connect dbt, Spark, Flink, your alerts or your own code | [Integrate](docs/integrate/index.md) |
| Write a connector | [Build a connector](docs/build-connectors/index.md) |
| Know how it works inside | [Internals](docs/internals/index.md) |

External connectors have their own versions and release cadence, separate from OpenETL. The official connectors and their docs live in [openetl-connectors](https://github.com/openetl-io/openetl-connectors); community and private connectors can live in their own repositories. [Choose and manage connectors](docs/pipelines/connectors.md) covers installation, compatibility and upgrades.

## Contributing

Issues and pull requests are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) has how to build and test OpenETL.

## License

[Apache 2.0](LICENSE)
