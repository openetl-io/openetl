---
title: Test connector checks
description: Test checks with unit tests, the CLI and the conformance kit.
---

Use these procedures after you [declare connector checks](./declare-checks.md).

## Test your checks

### In a unit test

`check.Validate` needs no database: building a list does no I/O. Test that each list your connector builds is valid. Here `testConfig` gives the test's config:

```go
func TestListsAreValid(t *testing.T) {
	src := &Source{cfg: testConfig(t)}
	if err := check.Validate(src.Checks()); err != nil {
		t.Error(err)
	}
	req := source.PreflightRequest{Streams: []source.ConfiguredStream{{Name: "public.orders"}, {Name: "public.refunds"}}}
	if err := check.Validate(src.Preflight(req)); err != nil {
		t.Error(err)
	}
}
```

Such a test catches names built from data that collide. For example, with the tables `public.orders` and `Public.Orders` in the config, `Validate` returns `check 6: the name "table.public-orders.read" is repeated`.

`check.Run` runs a list as the SDK does. Run it against a test database, and check the report:

```go
func TestPreflight(t *testing.T) {
	src := &Source{cfg: testConfig(t)}
	req := source.PreflightRequest{Streams: []source.ConfiguredStream{{Name: "public.orders"}}}
	r := check.Run(t.Context(), src.Preflight(req))
	for _, it := range r.Items {
		t.Logf("%s %s", it.Status, it.Name)
	}
	if !r.OK() {
		t.Fatal(r.Err())
	}
}
```

To test one check's `Run`, give it a zero `check.Probe`, and classify what it returns:

```go
func TestSlotMissing(t *testing.T) {
	src := &Source{cfg: testConfig(t)}
	c := src.slotCheck(src.kit())
	err := c.Run(t.Context(), &check.Probe{})
	if f := check.Classify(err); f == nil || f.Reason != "replication_slot_missing" {
		t.Fatalf("got %v, want replication_slot_missing", err)
	}
}
```

A zero `Probe` never runs its cleanups, so a kit's handle stays open until the test ends.

### With openetl connector check

`openetl connector check` starts your connector, sends `Check` a config, and prints the answer as people see it: one line for each item, with its templates filled. It needs no `openetld`. `-q` hides what the connector logs.

For example, take a source whose checks are `netcheck.Reach` of its `host` and `port`, then a check of its own that needs `net.connect`. Check it against a port where nothing listens:

```sh
echo '{"host": "localhost", "port": 1}' > config.json
openetl connector check -q --config config.json ./source-orders
```

```text
source-orders failed its check
  passed   Resolve localhost
           localhost resolves to ::1, 127.0.0.1.
  failed   Reach localhost:1
           localhost:1 refused the connection.
           detail: dial tcp [::1]:1: connect: connection refused
           fix: Check the host and port, and that the service listens there.
           field: port
           code: TRANSIENT (connection_refused)
  skipped  Read the orders on localhost
           needs net.connect, which failed
openetl: source-orders failed its check: TRANSIENT: Reach localhost:1: localhost:1 refused the connection: dial tcp [::1]:1: connect: connection refused
```

With `--format json`, it prints the answer as one JSON document, in the connector protocol's shape, with its templates unfilled. It exits with `0` when the check passed, warnings included, `1` when it failed, and `2` when it could not run, such as for a connector that did not start or answer.

### With the conformance kit

The [conformance kit](./test.md#test-it-with-the-conformance-kit) checks every answer to `Check` and to a preflight through the protocol, in any language. Its `check` and `preflight` checks fail if, with the suite's good config, an item fails or the response has an error. They log each warning. A connector that answers a preflight with `Unimplemented` skips the `preflight` check. A connector that answers with no items passes it.

The `errors` check runs `Check` with the suite's bad credentials, which must fail with `AUTH_INVALID`, and with an upstream that does not answer, which must fail with a code that can be retried. Each of these calls has 2 minutes. The check then gives `Check` 5 seconds with the upstream that does not answer. `Check` fails the conformance check if it runs out of time as an RPC, or answers more than 1 second late. Give the suite these configs with `BadCredentials` and `Unreachable`, or with `openetl connector test --bad-credentials` and `--unreachable`.

In each of these answers, the kit fails:

- An item without a name, with a name outside the grammar, or with the name of another item.
- An item without a title.
- A kind or a status that the protocol does not have, or no status.
- A reason that is not lowercase words joined by `_`.
- A failed item without an error, or a passed or skipped item with one.
- An error without a code, or whose message is not the item's message with its placeholders filled in.
- A title, description, message or fix with what looks like a placeholder but is not one, such as `{Host}`.
- A field that is not a property of the config schema, by its dotted path. A property of an array's items is under the array's path, with or without an index: `tables.name` or `tables.0.name`.
- A response with an error, no failed item, and no skipped item, when it has items. A response with a failed item but no error.
- A response's error whose code is not the first failed item's, or whose message is not that item's `title: message: detail`, with the placeholders filled in.
- Any text, param name or value, detail or error that holds the value of a secret in the suite's config. The kit looks at each template as written and filled in. Its report does not show such a text.

It flags, and does not fail, a placeholder without a param of its name.
