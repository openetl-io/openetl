# OpenETL

OpenETL moves data from where it's made to where it's used. It copies tables, changes and events from databases and streams into warehouses, lakes and other streams, on a schedule or continuously, and lands each change once wherever the destination can promise that.

You run it yourself. It's open source, under Apache 2.0.

> [!NOTE]
> OpenETL is pre-alpha: nothing has been released yet, and pipelines' format and the API can still change.

## What you can do with it

- **Copy from** Postgres (whole tables, new rows, or every change as it happens), Kafka and Kinesis.
- **Copy to** Postgres, Iceberg tables, Parquet files on S3, Kafka and Kinesis.
- **Choose how often:** the same pipeline runs every hour, every day, or continuously.
- **Trust what lands:** OpenETL works out, stream by stream, whether a change could ever arrive twice, and won't run a pipeline that would get less than it asks for.
- **Shape the data** with dbt after each sync, or on your own Spark and Flink clusters.
- **See what's happening** in the console: how fresh each table is, what failed and what to do about it. Metrics and traces go to anything that speaks OpenTelemetry.
- **Keep pipelines in git** as YAML, and review a plan of each change before you apply it.

## Get started

Until the first release, you run OpenETL from source. You need Git, [Go](https://go.dev/dl/) 1.26, [Rust](https://rustup.rs), [Node](https://nodejs.org) 22 with [pnpm](https://pnpm.io/installation), and [Docker](https://docs.docker.com/get-started/get-docker/).

```sh
git clone https://github.com/openetl-io/openetl
git clone https://github.com/openetl-io/openetl-connectors
cd openetl
make dev DEPS=postgres
```

The first run takes a few minutes to build. Then the console is at [localhost:8080](http://localhost:8080).

In a second terminal, in the same directory, create a pipeline that copies generated rows into that Postgres:

```sh
go build -o bin/ ./cmd/openetl && export PATH="$PWD/bin:$PATH"
echo postgres://openetl:openetl@localhost:5432/openetl | openetl secret set local-postgres-dsn
openetl apply -f examples/faker-to-postgres.yaml
```

Its first sync starts straight away. Open **Pipelines** in the console to see it land. The [quickstart](docs/get-started/quickstart.md) explains each step and what to try next.

## Documentation

| If you want to | Start with |
|---|---|
| Learn what OpenETL is, in plain words | [Overview](docs/get-started/overview.md) |
| Check on pipelines and the data they deliver | [Use the console](docs/use/index.md) |
| Build pipelines | [Pipelines](docs/pipelines/index.md) |
| Run OpenETL for your team | [Self-host](docs/self-host/index.md) |
| Connect dbt, Spark, Flink, your alerts or your own code | [Integrate](docs/integrate/index.md) |
| Write a connector | [Build a connector](docs/build-connectors/index.md) |
| Know how it works inside | [Internals](docs/internals/index.md) |

The official connectors, and their docs, live in [openetl-connectors](https://github.com/openetl-io/openetl-connectors).

## Contributing

Issues and pull requests are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) has how to build and test OpenETL.

## License

[Apache 2.0](LICENSE)
