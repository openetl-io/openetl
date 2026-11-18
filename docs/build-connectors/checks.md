---
title: Connector checks
description: Choose a guide for declaring, implementing and testing connection checks and preflights.
---

Checks examine a source or destination connection. Preflights examine the streams that a pipeline reads and writes.

| Your task | Guide |
|---|---|
| Add checks to your connector | [Declare connector checks](./declare-checks.md) |
| Use a network, HTTP, SQL or ADBC check | [Use check toolkits](./check-toolkits.md) |
| Implement connector-specific behavior | [Write custom checks](./write-checks.md) |
| Follow a complete implementation | [Complete check example](./check-example.md) |
| Look up failure reasons or protocol behavior | [Check reasons and protocol results](./check-results.md) |
| Verify your checks | [Test connector checks](./test-checks.md) |

For console instructions, see [Check connections and pipelines](../use/checks.md). For API fields, see [Check results in the API](../integrate/api-checks.md).

## Find a topic

Use these links to open a specific topic.

## Check and preflight

See [Check and preflight](./declare-checks.md#check-and-preflight).

## Declare checks

See [Declare checks](./declare-checks.md#declare-checks).

### On a source or destination you write

See [On a source or destination you write](./declare-checks.md#on-a-source-or-destination-you-write).

### Through sdk.Source

See [Through sdk.Source](./declare-checks.md#through-sdksource).

### The fields of a check

See [The fields of a check](./declare-checks.md#the-fields-of-a-check).

### Names

See [Names](./declare-checks.md#names).

### Kinds

See [Kinds](./declare-checks.md#kinds).

## Start from a toolkit

See [Start from a toolkit](./check-toolkits.md#start-from-a-toolkit).

### netcheck

See [netcheck](./check-toolkits.md#netcheck).

### httpcheck

See [httpcheck](./check-toolkits.md#httpcheck).

### sqlcheck

See [sqlcheck](./check-toolkits.md#sqlcheck).

### adbccheck

See [adbccheck](./check-toolkits.md#adbccheck).

### Combine toolkits

See [Combine toolkits](./check-toolkits.md#combine-toolkits).

## Write your own check

See [Write your own check](./write-checks.md#write-your-own-check).

### Reasons and failures

See [Reasons and failures](./write-checks.md#reasons-and-failures).

### Attach the cause

See [Attach the cause](./write-checks.md#attach-the-cause).

### Choose a code

See [Choose a code](./write-checks.md#choose-a-code).

### Params found at run time

See [Params found at run time](./write-checks.md#params-found-at-run-time).

### Reuse a toolkit's connection

See [Reuse a toolkit's connection](./write-checks.md#reuse-a-toolkits-connection).

### Classify driver errors

See [Classify driver errors](./write-checks.md#classify-driver-errors).

## Placeholders

See [Placeholders](./write-checks.md#placeholders).

## Order, skips, warnings and time

See [Order, skips, warnings and time](./write-checks.md#order-skips-warnings-and-time).

### Order and needs

See [Order and needs](./write-checks.md#order-and-needs).

### Skip

See [Skip](./write-checks.md#skip).

### Warnings

See [Warnings](./write-checks.md#warnings).

### Time limits

See [Time limits](./write-checks.md#time-limits).

### Cleanups

See [Cleanups](./write-checks.md#cleanups).

### Cancellation and panics

See [Cancellation and panics](./write-checks.md#cancellation-and-panics).

## A complete example

See [A complete example](./check-example.md#a-complete-example).

## The standard reasons

See [The standard reasons](./check-results.md#the-standard-reasons).

## What the platform does with the answer

See [What the platform does with the answer](./check-results.md#what-the-platform-does-with-the-answer).

## Connectors in other languages

See [Connectors in other languages](./check-results.md#connectors-in-other-languages).

## Test your checks

See [Test your checks](./test-checks.md#test-your-checks).

### In a unit test

See [In a unit test](./test-checks.md#in-a-unit-test).

### With openetl connector check

See [With openetl connector check](./test-checks.md#with-openetl-connector-check).

### With the conformance kit

See [With the conformance kit](./test-checks.md#with-the-conformance-kit).
