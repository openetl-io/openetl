package api

import (
	"testing"

	"github.com/openetl-io/openetl/internal/spec"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

func TestConfigForm(t *testing.T) {
	schema := `{"type":"object","x-groups":[{"id":"aws","title":"AWS"}],"properties":{
		"region":{"type":"string","x-group":"aws","description":"The region."},
		"auth":{"type":"object","x-group":"aws","properties":{
			"mode":{"type":"string","default":"keys","oneOf":[{"const":"keys","title":"Access keys"},{"const":"role","title":"A role"}]},
			"secret":{"type":"string","x-when":{"auth.mode":"keys"}}
		},"oneOf":[{"properties":{"secret":{"x-secret":true}}}]},
		"wait":{"type":"string","x-format":"duration","x-advanced":true,"minimum":1,"examples":["5s"]}
	}}`
	r := configForm(&spec.Role{Spec: &protocolv1.SpecResponse{ConfigSchema: []byte(schema)}})
	if len(r.GetGroups()) != 1 || r.GetGroups()[0].GetTitle() != "AWS" {
		t.Errorf("groups %v", r.GetGroups())
	}
	c := r.GetConfig()
	if len(c) != 3 || c[0].GetName() != "region" || c[1].GetName() != "auth" || c[2].GetName() != "wait" {
		t.Fatalf("fields %v", c)
	}
	auth := c[1].GetFields()
	if len(auth) != 2 || auth[0].GetGroup() != "aws" || auth[0].GetDefault() != `"keys"` || len(auth[0].GetOptions()) != 2 || auth[0].GetOptions()[1].GetTitle() != "A role" {
		t.Errorf("auth.mode %v", auth)
	}
	if s := auth[1]; s.GetPath() != "auth.secret" || !s.GetSecret() || len(s.GetWhen()) != 1 || s.GetWhen()[0].GetField() != "auth.mode" || s.GetWhen()[0].GetValues()[0] != `"keys"` {
		t.Errorf("auth.secret %v", s)
	}
	if w := c[2]; w.GetFormat() != "duration" || !w.GetAdvanced() || w.GetMinimum() != 1 || w.GetExamples()[0] != `"5s"` {
		t.Errorf("wait %v", w)
	}
}
