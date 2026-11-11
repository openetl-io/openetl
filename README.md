# OpenETL

OpenETL copies tables, changes and events from databases and event streams into warehouses, data lakes and other streams. Pipelines run on a schedule or continuously. OpenETL checks each stream's delivery guarantee and can commit each change exactly once when the connectors support it.

You run OpenETL on your own infrastructure. It is open source under Apache 2.0.

> [!NOTE]
> OpenETL is in alpha. The pipeline format and the API can change between releases.

## What you can do with it

- **Connect your systems** with source and destination connectors. Use an official connector, one maintained by the community, or one your organization builds for its own systems.
- **Choose how often:** the same pipeline runs every hour, every day, or continuously.
- **Set delivery requirements:** OpenETL checks whether each stream can receive duplicate changes. It refuses to run a pipeline that cannot meet its required guarantee.
- **Shape the data** with dbt after each sync, or on your own Spark and Flink clusters.
- **Monitor pipelines** in the console. Check data freshness, failures and recovery steps. Export metrics and traces through OpenTelemetry.
- **Keep pipelines in git** as YAML, and review a plan of each change before you apply it.

## Get started

You need macOS or Linux, and [Docker](https://docs.docker.com/get-started/get-docker/) with Compose. Install the `openetl` CLI:

```sh
curl -fsSL https://raw.githubusercontent.com/openetl-io/openetl/main/install.sh | sh
```

Start OpenETL 0.1.0-alpha with Docker Compose, in a new directory:

```sh
mkdir openetl && cd openetl
curl -fsSLO https://raw.githubusercontent.com/openetl-io/openetl/v0.1.0-alpha/deploy/compose/compose.yml
cat > .env <<EOF
OPENETL_IMAGE=ghcr.io/openetl-io/openetl:0.1.0-alpha
POSTGRES_PASSWORD=$(openssl rand -hex 16)
OPENETL_SECRETS_KEY=base64key://$(openssl rand -base64 32 | tr '+/' '-_')
EOF
docker compose up -d
```

The console is at [localhost:8080](http://localhost:8080). The [quickstart](docs/get-started/quickstart.md) continues from here: it signs you in, installs a connector, and copies generated rows into Postgres.

To run OpenETL for your team, follow [Self-host](docs/self-host/index.md). To build OpenETL from source, see [CONTRIBUTING.md](CONTRIBUTING.md).

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
