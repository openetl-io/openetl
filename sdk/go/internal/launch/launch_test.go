package launch

import "testing"

func TestParse(t *testing.T) {
	for in, want := range map[string]string{
		"unix:///tmp/c.sock":   "unix:///tmp/c.sock",
		"tcp://127.0.0.1:7700": "passthrough:///127.0.0.1:7700",
		"tcp://[::1]:7700":     "passthrough:///[::1]:7700",
		"unix://c.sock":        "",
		"tcp://127.0.0.1":      "",
		"/tmp/c.sock":          "",
		"http://127.0.0.1:80":  "",
	} {
		a, err := Parse(in)
		switch {
		case want == "" && err == nil:
			t.Errorf("%s parsed as %v", in, a)
		case want != "" && (err != nil || a.Target() != want || a.String() != in):
			t.Errorf("%s: %v, %v; want %s", in, a, err, want)
		}
	}
}
