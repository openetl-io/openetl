---
title: Complete check example
description: Follow a complete connector check example with its recorded output.
---

Read [Declare connector checks](./declare-checks.md) before you use this example.

## A complete example

This source reads a PostgreSQL database's changes through a replication slot and a publication. The example is not an official connector: it shows how a connector's checks fit together. The rest of the source, its `Discover` and `Read`, is not shown.

Its check combines `sqlcheck` checks with one of its own:

- `db.connect` connects.
- `role.replication` checks a role attribute with `True`.
- `setting.wal-level` checks a setting with `Value`, and `setting.max-slot-wal-keep-size` warns about another, as an advisory check.
- One `Readable` check for each configured table, named with `check.Name`.
- `replication.slot` is its own check, with its own reasons and the kit's connection.

Its preflight makes one check for each stream that the pipeline reads: that the stream's table is in the publication.

```go
package pgsource

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/check/sqlcheck"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type Config struct {
	Host        string   `json:"host"`
	Port        int      `json:"port"`
	Database    string   `json:"database"`
	User        string   `json:"user"`
	Password    string   `json:"password"`
	Slot        string   `json:"slot"`
	Publication string   `json:"publication"`
	Tables      []string `json:"tables"`
}

type Source struct {
	cfg Config
}

var (
	_ source.CheckLister = (*Source)(nil)
	_ source.Preflighter = (*Source)(nil)
)

var (
	slotMissing = check.Reason{
		Name:    "replication_slot_missing",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The replication slot {slot} doesn't exist on {host}.",
		Fix:     "Create it: SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput');",
	}
	slotPlugin = check.Reason{
		Name:    "replication_slot_plugin",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The replication slot {slot} decodes with {plugin}, and the connector needs pgoutput.",
		Fix:     "Drop the slot and create it again with the plugin pgoutput.",
	}
	notPublished = check.Reason{
		Name:    "table_not_published",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{stream} isn't in the publication {publication}.",
		Fix:     "Run ALTER PUBLICATION {publication} ADD TABLE {stream};",
	}
)

// kit is new for each list, so each run opens one handle and closes it.
func (s *Source) kit() *sqlcheck.Kit {
	dsn := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(s.cfg.User, s.cfg.Password),
		Host:   net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.cfg.Port)),
		Path:   s.cfg.Database,
	}
	return sqlcheck.New(func(ctx context.Context) (*sql.DB, error) {
		return sql.Open("pgx", dsn.String())
	})
}

func (s *Source) Checks() []check.Check {
	k := s.kit()

	noReplication := check.PrivilegeMissing.Fail("principal", s.cfg.User, "privilege", "REPLICATION", "object", "the server")
	noReplication.Fix = "Run ALTER ROLE {principal} WITH REPLICATION;"
	replication := k.True("role.replication", "Stream changes as {user}", noReplication, nil,
		`SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user`)
	replication.Params = check.Params{"user": s.cfg.User}
	replication.Field = "user"

	walLevel := k.Value("setting.wal-level", "Decode the write-ahead log", func(v string) error {
		if v != "logical" {
			return check.SettingMismatch.Fail("setting", "wal_level", "required", "logical")
		}
		return nil
	}, `SHOW wal_level`)

	walKeep := k.Value("setting.max-slot-wal-keep-size", "Bound the WAL a slot keeps", func(v string) error {
		if v == "-1" {
			return check.SettingMismatch.Fail("setting", "max_slot_wal_keep_size", "required", "a limit, such as 100GB")
		}
		return nil
	}, `SHOW max_slot_wal_keep_size`)
	walKeep.Advisory = true
	walKeep.Description = "A slot that falls behind keeps WAL on the server's disk until this limit."

	checks := []check.Check{k.Connect(), replication, walLevel, walKeep}
	for _, t := range s.cfg.Tables {
		read := k.Readable(check.Name("table", t, "read"), pgx.Identifier(strings.Split(t, ".")).Sanitize())
		read.Field = "tables"
		checks = append(checks, read)
	}
	return append(checks, s.slotCheck(k))
}

func (s *Source) slotCheck(k *sqlcheck.Kit) check.Check {
	c := k.Check("replication.slot", check.Resource, "Find the replication slot {slot}",
		func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			var plugin string
			err := db.QueryRowContext(ctx, `SELECT plugin FROM pg_replication_slots WHERE slot_name = $1`, s.cfg.Slot).Scan(&plugin)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				return slotMissing.Fail()
			case err != nil:
				return k.Failure(err)
			}
			p.Param("plugin", plugin)
			if plugin != "pgoutput" {
				return slotPlugin.Fail()
			}
			p.Found("{slot} decodes with {plugin}.")
			return nil
		})
	c.Params = check.Params{"slot": s.cfg.Slot, "host": s.cfg.Host}
	c.Field = "slot"
	return c
}

func (s *Source) Preflight(req source.PreflightRequest) []check.Check {
	k := s.kit()
	checks := []check.Check{k.Connect()}
	for _, st := range req.Streams {
		c := k.True(check.Name("stream", st.Name, "published"), "Publish {stream} in {publication}", notPublished.Fail(), nil,
			`SELECT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = $1 AND schemaname || '.' || tablename = $2)`,
			s.cfg.Publication, st.Name)
		c.Params = check.Params{"stream": st.Name, "publication": s.cfg.Publication}
		c.Field = "publication"
		checks = append(checks, c)
	}
	return checks
}
```

Some details of the example:

- `slotMissing.Fail()` and `notPublished.Fail()` take no params. The check's params fill their placeholders.
- `slotPlugin`'s `{plugin}` comes from `p.Param`, which the check calls before it fails.
- The password is in the DSN, but not in a param or a template. Mark `password` with `"x-secret": true` in the config schema, and serve the connector on the `EgressRaw` tier. The SDK then puts the secret's value in the config, and removes it from the answers.

To see the items, run each list with `check.Run`, and print each item with its templates filled in:

```go
func show(r check.Report) {
	for _, it := range r.Items {
		it = it.Rendered()
		fmt.Printf("%-8s %s: %s\n", it.Status, it.Name, it.Title)
		if it.Message != "" {
			fmt.Printf("         %s\n", it.Message)
		}
		if f := it.Failure; f != nil {
			if f.Detail != "" {
				fmt.Printf("         detail: %s\n", f.Detail)
			}
			if f.Fix != "" {
				fmt.Printf("         fix: %s\n", f.Fix)
			}
		}
	}
	fmt.Println("error:", r.Err())
}
```

The config here has the host `db.example.com`, the user `openetl`, the slot `openetl_orders`, the publication `openetl` and the tables `public.orders` and `public.refunds`. The database has `wal_level` set to `logical` and `max_slot_wal_keep_size` set to `-1`. The user can read `public.orders` but not `public.refunds`. The slot does not exist, and the publication has `public.orders` only. The check, `show(check.Run(ctx, src.Checks()))`, prints:

```text
passed   db.connect: Connect to the database
passed   role.replication: Stream changes as openetl
passed   setting.wal-level: Decode the write-ahead log
warning  setting.max-slot-wal-keep-size: Bound the WAL a slot keeps
         max_slot_wal_keep_size is -1, and the connector needs a limit, such as 100GB.
         fix: Set max_slot_wal_keep_size to a limit, such as 100GB.
passed   table.public-orders.read: Read "public"."orders"
failed   table.public-refunds.read: Read "public"."refunds"
         The database user lacks a privilege this needs.
         detail: ERROR: permission denied for table refunds (SQLSTATE 42501)
         fix: Grant the connector's database user the privilege the detail names.
failed   replication.slot: Find the replication slot openetl_orders
         The replication slot openetl_orders doesn't exist on db.example.com.
         fix: Create it: SELECT pg_create_logical_replication_slot('openetl_orders', 'pgoutput');
error: Read "public"."refunds": The database user lacks a privilege this needs: ERROR: permission denied for table refunds (SQLSTATE 42501)
```

A preflight of the streams `public.orders` and `public.refunds` prints:

```text
passed   db.connect: Connect to the database
passed   stream.public-orders.published: Publish public.orders in openetl
failed   stream.public-refunds.published: Publish public.refunds in openetl
         public.refunds isn't in the publication openetl.
         fix: Run ALTER PUBLICATION openetl ADD TABLE public.refunds;
error: Publish public.refunds in openetl: public.refunds isn't in the publication openetl.
```

The `error` line is the report's `Err`: the first failed item, as a host from before items shows it.
