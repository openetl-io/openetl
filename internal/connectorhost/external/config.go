package external

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"

	"github.com/openetl-io/openetl/internal/connectorhost/hostservices"
	"github.com/openetl-io/openetl/internal/platform/errs"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/configschema"
)

// splitSecrets moves the config's secrets, the properties the schema marks
// x-secret at any depth, into host services, and leaves each one's dotted
// path in its place as the secret's name.
func splitSecrets(schema, config []byte) ([]byte, hostservices.StaticSecrets, error) {
	secrets := hostservices.StaticSecrets{}
	if len(schema) == 0 || len(bytes.TrimSpace(config)) == 0 {
		return config, secrets, nil
	}
	props, err := SecretProperties(schema)
	if err != nil {
		return nil, nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, nil, errs.New(errs.ConfigInvalid, "connector: the config isn't a JSON object")
	}
	for _, k := range props {
		v, ok := configschema.Lookup(m, k)
		if !ok {
			continue
		}
		value, ok := v.(string)
		if !ok {
			return nil, nil, errs.New(errs.ConfigInvalid, "connector: config.%s is a secret, so a string", k)
		}
		secrets[k] = sdk.NewSecret(value)
		configschema.Replace(m, k, k)
	}
	out, err := json.Marshal(m)
	return out, secrets, err
}

// SecretProperties lists the properties a config schema marks x-secret, as
// dotted paths, at any depth. A secret inside an array is an error.
func SecretProperties(schema []byte) ([]string, error) {
	props, err := configschema.SecretPaths(schema)
	return props, errs.Wrap(errs.ConfigInvalid, err, "connector")
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
