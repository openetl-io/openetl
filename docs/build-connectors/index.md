---
title: Build a connector
description: Choose a task for implementing, checking, testing or publishing a connector.
---

A connector reads from or writes to a system. OpenETL starts it, supplies its configuration and moves records through the data engine.

You can maintain a connector in your own repository and release it on your own schedule. You can distribute it publicly or privately.

The official [openetl-connectors](https://github.com/openetl-io/openetl-connectors) use the same protocol and tools.

| Your task | Guide |
|---|---|
| Understand source and destination responsibilities | [Connector contracts](./protocol.md) |
| Build your first external source | [Write a source in Go](./write-source.md) |
| Add connection checks and preflights | [Connector checks](./checks.md) |
| Verify protocol behavior | [Test connector conformance](./test.md) |
| Prepare and distribute a release | [Package and publish a connector](./publish.md) |

This section assumes Go or another language with gRPC support.

## Find a topic

Use these links to open a specific topic.

## Three kinds of connector

See [Three kinds of connector](./protocol.md#three-kinds-of-connector).

## The contract

See [The contract](./protocol.md#the-contract).

### A source

See [A source](./protocol.md#a-source).

### A destination

See [A destination](./protocol.md#a-destination).

### Setup and teardown

See [Setup and teardown](./protocol.md#setup-and-teardown).

### Checks and preflights

See [Checks and preflights](./protocol.md#checks-and-preflights).

### The config schema

See [The config schema](./protocol.md#the-config-schema).

### Jobs on several workers

See [Jobs on several workers](./protocol.md#jobs-on-several-workers).

#### Lane commits

See [Lane commits](./protocol.md#lane-commits).

## Write a source in Go

See [Write a source in Go](./write-source.md#write-a-source-in-go).

## Try it with the CLI

See [Try it with the CLI](./write-source.md#try-it-with-the-cli).

## Test it with the conformance kit

See [Test it with the conformance kit](./test.md#test-it-with-the-conformance-kit).

## Use it in OpenETL

See [Use it in OpenETL](./publish.md#use-it-in-openetl).

## Package and publish

See [Package and publish](./publish.md#package-and-publish).

## Release and compatibility

See [Release and compatibility](./publish.md#release-and-compatibility).

### Private distribution

See [Private distribution](./publish.md#private-distribution).

## Document it

See [Document it](./publish.md#document-it).
