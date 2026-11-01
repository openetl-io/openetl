package external

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/unsuitable001/openetl/internal/connectorhost/hostservices"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	sdk "github.com/unsuitable001/openetl/sdk/go"
)

// splitSecrets moves the config's secrets, the top-level properties the
// schema marks x-secret, into host services, and leaves each one's property
// name in its place as the secret's name.
func splitSecrets(schema, config []byte) ([]byte, hostservices.StaticSecrets, error) {
	secrets := hostservices.StaticSecrets{}
	if len(schema) == 0 || len(bytes.TrimSpace(config)) == 0 {
		return config, secrets, nil
	}
	var s struct {
		Properties map[string]struct {
			Secret bool `json:"x-secret"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(schema, &s); err != nil {
		return nil, nil, errs.Wrap(errs.ConfigInvalid, err, "connector: config schema")
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(config, &m); err != nil {
		return nil, nil, errs.New(errs.ConfigInvalid, "connector: the config isn't a JSON object")
	}
	for k, p := range s.Properties {
		v, ok := m[k]
		if !p.Secret || !ok {
			continue
		}
		var value string
		if err := json.Unmarshal(v, &value); err != nil {
			return nil, nil, errs.New(errs.ConfigInvalid, "connector: config.%s is a secret, so a string", k)
		}
		secrets[k] = sdk.NewSecret(value)
		name, err := json.Marshal(k)
		if err != nil {
			return nil, nil, err
		}
		m[k] = name
	}
	out, err := json.Marshal(m)
	return out, secrets, err
}

// CompileSchema checks that schema, a connector's config schema, is a JSON
// Schema. It loads nothing from the network.
func CompileSchema(schema []byte) (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schema))
	if err != nil {
		return nil, err
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource("config.schema.json", doc); err != nil {
		return nil, err
	}
	return c.Compile("config.schema.json")
}

// ValidateConfig checks config, a JSON object, against a connector's config
// schema. Errors leave out the config's secrets.
func ValidateConfig(name string, schema, config []byte) error {
	sch, err := CompileSchema(schema)
	if err != nil {
		return errs.Wrap(errs.ConfigInvalid, err, name+": config schema")
	}
	if len(bytes.TrimSpace(config)) == 0 {
		config = []byte("{}")
	}
	v, err := jsonschema.UnmarshalJSON(bytes.NewReader(config))
	if err != nil {
		return errs.Wrap(errs.ConfigInvalid, err, name+": config")
	}
	if err := sch.Validate(v); err != nil {
		msg := err.Error()
		if _, secrets, serr := splitSecrets(schema, config); serr == nil {
			for _, s := range secrets {
				if r := s.Reveal(); r != "" {
					msg = strings.ReplaceAll(msg, r, "[redacted]")
				}
			}
		}
		return errs.New(errs.ConfigInvalid, "%s: config: %s", name, msg)
	}
	return nil
}
