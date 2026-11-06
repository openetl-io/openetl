// Command both is a connector with both roles, for the launchers' tests. Its
// source reads one record: where it was told to listen, and what host
// services hold under the key "k".
package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func main() {
	sdk.Serve(sdk.Connector{
		Name:         "both",
		ConfigSchema: []byte(`{"type": "object", "properties": {"token": {"type": "string", "x-secret": true}}}`),
		Source:       func(_ []byte, h host.Host) (source.Source, error) { return src{h}, nil },
		Destination: func([]byte, host.Host) (destination.Destination, error) {
			return nil, errors.New("only its spec")
		},
		Capabilities: destination.Capabilities{Class: destination.DeliverAndAck},
	})
}

type src struct{ h host.Host }

func (src) Discover(context.Context) (source.Catalog, error) {
	return source.Catalog{Streams: []source.CatalogStream{{Name: "launch", Replay: source.Exact}}}, nil
}

func (s src) Read(ctx context.Context, _ source.ReadRequest, out source.Emitter) error {
	v, _, err := s.h.KVGet(ctx, "k")
	if err != nil {
		return err
	}
	row, _ := json.Marshal(map[string]string{"addr": os.Getenv(sdk.EnvConnectorAddr), "k": string(v)})
	if err := out.Rows("launch", []json.RawMessage{row}); err != nil {
		return err
	}
	return out.Complete("launch", nil)
}
