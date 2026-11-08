package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// object is a JSON object that keeps its keys in the document's order, so
// a page lists fields in the order the schema does.
type object struct {
	keys []string
	vals map[string]any
}

func (o *object) has(k string) bool {
	_, ok := o.vals[k]
	return ok
}

func (o *object) str(k string) string {
	s, _ := o.vals[k].(string)
	return s
}

func (o *object) obj(k string) *object {
	v, _ := o.vals[k].(*object)
	return v
}

func (o *object) list(k string) []any {
	v, _ := o.vals[k].([]any)
	return v
}

func (o *object) strings(k string) []string {
	var out []string
	for _, v := range o.list(k) {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// decode reads a JSON document whose top level is an object. Numbers stay
// json.Number, so they print as the schema writes them.
func decode(b []byte) (*object, error) {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	v, err := value(d)
	if err != nil {
		return nil, err
	}
	o, ok := v.(*object)
	if !ok {
		return nil, errors.New("the schema isn't an object")
	}
	return o, nil
}

func value(d *json.Decoder) (any, error) {
	t, err := d.Token()
	if err != nil {
		return nil, err
	}
	switch t {
	case json.Delim('{'):
		o := &object{vals: map[string]any{}}
		for d.More() {
			k, err := d.Token()
			if err != nil {
				return nil, err
			}
			key, ok := k.(string)
			if !ok {
				return nil, fmt.Errorf("a key is %v", k)
			}
			v, err := value(d)
			if err != nil {
				return nil, err
			}
			o.keys = append(o.keys, key)
			o.vals[key] = v
		}
		_, err := d.Token()
		return o, err
	case json.Delim('['):
		var l []any
		for d.More() {
			v, err := value(d)
			if err != nil {
				return nil, err
			}
			l = append(l, v)
		}
		_, err := d.Token()
		return l, err
	}
	return t, nil
}
