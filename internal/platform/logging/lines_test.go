package logging

import (
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"testing/iotest"
)

func TestLines(t *testing.T) {
	const cutAt = " [line cut at 8 bytes]"
	wrappers := map[string]func(io.Reader) io.Reader{
		"whole":         func(r io.Reader) io.Reader { return r },
		"byte by byte":  iotest.OneByteReader,
		"halves":        iotest.HalfReader,
		"data with EOF": iotest.DataErrReader,
	}
	for _, tc := range []struct {
		name, in string
		want     []string
	}{
		{name: "nothing", in: ""},
		{name: "lines", in: "a\nbb\n", want: []string{"a", "bb"}},
		{name: "carriage returns", in: "a\r\nb\r\n", want: []string{"a", "b"}},
		{name: "empty lines", in: "\n\nx\n", want: []string{"", "", "x"}},
		{name: "no newline at the end", in: "a\nbc", want: []string{"a", "bc"}},
		{name: "just under the limit", in: "1234567\nx\n", want: []string{"1234567", "x"}},
		{name: "at the limit", in: "12345678\nx\n", want: []string{"12345678" + cutAt, "x"}},
		{name: "a long line", in: "1234567890123456789012345\nafter\n", want: []string{"12345678" + cutAt, "after"}},
		{name: "long lines in a row", in: "1234567890\nabcdefghij\nz\n", want: []string{"12345678" + cutAt, "abcdefgh" + cutAt, "z"}},
		{name: "a long line and nothing after it", in: "1234567890123", want: []string{"12345678" + cutAt}},
		{name: "a long line's tail and a short last line", in: "1234567890\nlast", want: []string{"12345678" + cutAt, "last"}},
	} {
		for wrapper, wrap := range wrappers {
			t.Run(tc.name+"/"+wrapper, func(t *testing.T) {
				sc := linesUpTo(wrap(strings.NewReader(tc.in)), 8)
				var got []string
				for sc.Scan() {
					got = append(got, sc.Text())
				}
				if err := sc.Err(); err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(got, tc.want) {
					t.Errorf("got %q, want %q", got, tc.want)
				}
			})
		}
	}
}

func TestLinesReportsReadErrors(t *testing.T) {
	boom := errors.New("the pipe broke")
	sc := Lines(io.MultiReader(strings.NewReader("a\n"), iotest.ErrReader(boom)))
	if !sc.Scan() || sc.Text() != "a" {
		t.Fatalf("got %q", sc.Text())
	}
	if sc.Scan() || !errors.Is(sc.Err(), boom) {
		t.Errorf("got %q and %v", sc.Text(), sc.Err())
	}
}
