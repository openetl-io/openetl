package api

import (
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestBindSecrets(t *testing.T) {
	config := func(js string) *structpb.Struct {
		s := &structpb.Struct{}
		if err := protojson.Unmarshal([]byte(js), s); err != nil {
			t.Fatal(err)
		}
		return s
	}
	for _, tc := range []struct {
		name, config string
		props        []string
		known        bool
		want         []string
		masked       string
		err          string
	}{
		{name: "a secret property names one", config: `{"dsn": {"secret": "pg"}, "schema": "public"}`, props: []string{"dsn"}, known: true,
			want: []string{"pg"}, masked: `{"dsn":"secret","schema":"public"}`},
		{name: "two properties, one secret", config: `{"a": {"secret": "s"}, "b": {"secret": "s"}}`, props: []string{"a", "b"}, known: true,
			want: []string{"s"}, masked: `{"a":"secret","b":"secret"}`},
		{name: "unset", config: `{"schema": "public"}`, props: []string{"dsn"}, known: true, want: []string{}, masked: `{"schema":"public"}`},
		{name: "a value", config: `{"dsn": "postgres://u:p@h/db"}`, props: []string{"dsn"}, known: true, err: "config.dsn is a secret"},
		{name: "a malformed reference", config: `{"dsn": {"secret": "pg", "x": 1}}`, props: []string{"dsn"}, known: true, err: "config.dsn is a secret"},
		{name: "a number as the name", config: `{"dsn": {"secret": 1}}`, props: []string{"dsn"}, known: true, err: "config.dsn is a secret"},
		{name: "a bad name", config: `{"dsn": {"secret": "Bad_Name"}}`, props: []string{"dsn"}, known: true, err: "lowercase"},
		{name: "not a secret property", config: `{"opts": {"secret": "x"}}`, known: true, want: []string{}, masked: `{"opts":{"secret":"x"}}`},
		{name: "an unknown schema", config: `{"dsn": {"secret": "pg"}, "token": "t"}`, want: []string{"pg"}, masked: `{"dsn":"secret","token":"t"}`},
		{name: "a nested secret", config: `{"auth": {"oauth": {"client_secret": {"secret": "cs"}, "client_id": "id"}}}`, props: []string{"auth.oauth.client_secret", "auth.password"}, known: true,
			want: []string{"cs"}, masked: `{"auth":{"oauth":{"client_id":"id","client_secret":"secret"}}}`},
		{name: "a nested value", config: `{"auth": {"password": "hunter2"}}`, props: []string{"auth.password"}, known: true, err: "config.auth.password is a secret"},
		{name: "nested, unknown schema", config: `{"auth": {"token": {"secret": "tk"}}, "hosts": [{"secret": "no"}]}`, want: []string{"tk"},
			masked: `{"auth":{"token":"secret"},"hosts":[{"secret":"no"}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			names, masked, err := bindSecrets(config(tc.config), tc.props, tc.known)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("err = %v, want %q", err, tc.err)
				}
				if strings.Contains(err.Error(), "postgres://") {
					t.Errorf("the error quotes the value: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(names, tc.want) {
				t.Errorf("names = %q, want %q", names, tc.want)
			}
			b, err := protojson.Marshal(masked)
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.ReplaceAll(string(b), " ", ""); got != tc.masked {
				t.Errorf("masked = %s, want %s", got, tc.masked)
			}
		})
	}
}
