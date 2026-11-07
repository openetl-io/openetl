package spec

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// fill sets every field of m, found by reflection, to a value that isn't its
// zero, and the same value every time.
func fill(t *testing.T, m protoreflect.Message) {
	t.Helper()
	fields := m.Descriptor().Fields()
	for i := range fields.Len() {
		f := fields.Get(i)
		if f.IsMap() {
			t.Fatalf("%s is a map, which fill doesn't fill", f.FullName())
		}
		if f.Kind() == protoreflect.MessageKind {
			if f.Message().FullName() == "google.protobuf.Duration" {
				m.Set(f, protoreflect.ValueOfMessage(durationpb.New(90e9).ProtoReflect()))
				continue
			}
			if f.IsList() {
				t.Fatalf("%s is a list of messages, which fill doesn't fill", f.FullName())
			}
			fill(t, m.Mutable(f).Message())
			continue
		}
		var v protoreflect.Value
		switch f.Kind() {
		case protoreflect.BoolKind:
			v = protoreflect.ValueOfBool(true)
		case protoreflect.StringKind:
			v = protoreflect.ValueOfString("x-" + string(f.Name()))
		case protoreflect.BytesKind:
			v = protoreflect.ValueOfBytes([]byte(`{"type":"object","properties":{"a":{"type":"integer","minimum":1}}}`))
		case protoreflect.EnumKind:
			vals := f.Enum().Values()
			v = protoreflect.ValueOfEnum(vals.Get(vals.Len() - 1).Number())
		case protoreflect.Uint32Kind:
			v = protoreflect.ValueOfUint32(7)
		case protoreflect.Int32Kind:
			v = protoreflect.ValueOfInt32(7)
		case protoreflect.Uint64Kind:
			v = protoreflect.ValueOfUint64(7)
		case protoreflect.Int64Kind:
			v = protoreflect.ValueOfInt64(7)
		default:
			t.Fatalf("%s is a %s, which fill doesn't fill", f.FullName(), f.Kind())
		}
		if f.IsList() {
			m.Mutable(f).List().Append(v)
			continue
		}
		m.Set(f, v)
	}
}

// Every field Spec returns survives a descriptor, both ways, so a field added
// to the protocol fails here until the descriptor carries it.
func TestSpecFidelity(t *testing.T) {
	for _, role := range []string{RoleSource, RoleDestination} {
		t.Run(role, func(t *testing.T) {
			s := &protocolv1.SpecResponse{}
			fill(t, s.ProtoReflect())
			s.ProtocolVersion, s.Kind, s.RequiredFeatures = "v1", KindOf(role), []string{"some-feature"}
			if role == RoleSource {
				s.Destination = nil
			} else {
				s.Source = nil
			}
			doc, err := MarshalDescriptor("acme/full", "1.0.0", Info{Repository: "https://github.com/acme/full"}, &Descriptor{Roles: map[string]*Role{role: {
				Requires: Requires{Protocol: "v1"},
				Binaries: map[string]Binary{"linux/amd64": {URL: "https://example.com/x", Digest: "sha256:" + string(make64('a'))}},
				Spec:     s,
			}}})
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := Parse(doc)
			if err != nil {
				t.Fatalf("%v\n%s", err, doc)
			}
			got := parsed.Descriptor.Roles[role].Spec
			if diff := SameSpec(got, s); diff != "" {
				t.Errorf("the descriptor drops a field: %s", diff)
			}
			got.ConfigSchema, s.ConfigSchema = nil, nil
			if !proto.Equal(got, s) {
				t.Errorf("got %v\nwant %v", got, s)
			}
		})
	}
}

// SameSpec sees a difference in any field.
func TestSameSpecEveryField(t *testing.T) {
	base := &protocolv1.SpecResponse{}
	fill(t, base.ProtoReflect())
	var walk func(m protoreflect.Message, path string)
	walk = func(m protoreflect.Message, path string) {
		fields := m.Descriptor().Fields()
		for i := range fields.Len() {
			f := fields.Get(i)
			name := path + string(f.Name())
			if f.Kind() == protoreflect.MessageKind && f.Message().FullName() != "google.protobuf.Duration" {
				walk(m.Get(f).Message(), name+".")
				continue
			}
			if name == "config_schema" {
				continue
			}
			other := proto.CloneOf(base)
			clearPath(other.ProtoReflect(), name)
			if SameSpec(other, base) == "" {
				t.Errorf("SameSpec misses %s", name)
			}
		}
	}
	walk(base.ProtoReflect(), "")
}

func clearPath(m protoreflect.Message, path string) {
	fields := m.Descriptor().Fields()
	for i := range fields.Len() {
		f := fields.Get(i)
		name := string(f.Name())
		if path == name {
			m.Clear(f)
			return
		}
		if len(path) > len(name) && path[:len(name)+1] == name+"." {
			clearPath(m.Mutable(f).Message(), path[len(name)+1:])
			return
		}
	}
}

func make64(c byte) []byte {
	b := make([]byte, 64)
	for i := range b {
		b[i] = c
	}
	return b
}
