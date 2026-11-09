// Command refdocs writes the reference pages that come from a source of
// truth in the code: openetld's settings, and the JSON Schemas of
// openetl-connector.yaml, a Connector descriptor, a DeclarativeSource
// manifest and a platform event. Run it from the repository's root:
//
//	go run ./internal/refdocs
//
// TestPagesAreCurrent fails while a page differs from what it writes.
package main

import (
	"cmp"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"

	"github.com/openetl-io/openetl/internal/connectorpkg"
	"github.com/openetl-io/openetl/internal/declarative"
	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/spec"
)

// The command that writes the pages, for messages.
const command = "go run ./internal/refdocs"

type page struct {
	// Its file in docs/reference.
	file        string
	title       string
	description string
	// Paragraphs before the generated part, in Markdown.
	about []string
	// The file it's generated from, relative to the repository's root.
	source string
	schema func() []byte
	// What the page says of fields it doesn't list; empty for a document
	// OpenETL reads, which refuses them.
	unlisted string
}

const refused = "A field that isn't listed is refused, unless its section says other fields are allowed."

var schemaPages = []page{
	{
		file:        "connector-file.md",
		title:       "openetl-connector.yaml",
		description: "Every field of the file a connector package is built from.",
		about: []string{
			"The file is `openetl-connector.yaml` at the root of the package's directory. `openetl connector validate` checks it without building, and the descriptor `openetl connector package` writes for each version is a [Connector descriptor](./connector-descriptor.md). `openetl connector schema file` prints its JSON Schema, for an editor to check the file as you write it. [Build a connector](../build-connectors/index.md#package-and-publish) shows one.",
		},
		source: "internal/connectorpkg/openetl-connector.schema.json",
		schema: connectorpkg.Schema,
	},
	{
		file:        "connector-descriptor.md",
		title:       "Connector descriptor",
		description: "Every field of a Connector document, which describes one version of a connector package.",
		about: []string{
			"`openetl connector apply` applies a descriptor to the org, from a file or by a package version's reference, and runners fetch each role's executable or image from what it names. `openetl connector schema descriptor` prints its JSON Schema.",
		},
		source: "internal/spec/connector.schema.json",
		schema: spec.DescriptorSchema,
	},
	{
		file:        "manifest.md",
		title:       "Declarative source manifest",
		description: "Every field of a DeclarativeSource manifest.",
		about: []string{
			"A manifest is a whole source for an HTTP API, which OpenETL runs itself, with no executable. Apply one with `openetl connector apply -f FILE`, or name one as a role's `manifest` in [`openetl-connector.yaml`](./connector-file.md). `openetl connector schema manifest` prints its JSON Schema.",
		},
		source: "internal/declarative/manifest.schema.json",
		schema: declarative.Schema,
	},
	{
		file:        "events.md",
		title:       "Platform events",
		description: "Every platform event's type, and every field of its data.",
		about: []string{
			"[Webhooks](../integrate/index.md#events-and-webhooks) deliver the events you choose, each as one `POST` with this JSON body. `type` says which section below applies to `data`.",
		},
		source:   "internal/events/event.schema.json",
		schema:   events.PayloadSchema,
		unlisted: "The schema allows fields it doesn't list, so ignore any you don't know.",
	},
}

func main() {
	out := flag.String("out", "docs/reference", "the directory the pages go in")
	flag.Parse()
	pages, err := render()
	if err != nil {
		fmt.Fprintln(os.Stderr, "refdocs:", err)
		os.Exit(1)
	}
	root, err := os.OpenRoot(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "refdocs:", err)
		os.Exit(1)
	}
	defer func() { _ = root.Close() }()
	for _, p := range pages {
		if err := root.WriteFile(p.file, []byte(p.body), 0o644); err != nil {
			fmt.Fprintln(os.Stderr, "refdocs:", err)
			os.Exit(1)
		}
		fmt.Println(filepath.Join(*out, p.file))
	}
}

type rendered struct {
	file, body string
}

// render returns every page, in Markdown.
func render() ([]rendered, error) {
	var out []rendered
	for _, p := range schemaPages {
		d, err := newSchemaDoc(p.schema())
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p.source, err)
		}
		body, err := d.render()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p.source, err)
		}
		var b strings.Builder
		front(&b, p.title, p.description)
		if desc := text(d.root.str("description")); desc != "" {
			b.WriteString(desc + "\n\n")
		}
		for _, a := range p.about {
			b.WriteString(a + "\n\n")
		}
		fmt.Fprintf(&b, "This page is generated from [`%s`](../../%s). %s\n\n", p.source, p.source, cmp.Or(p.unlisted, refused))
		b.WriteString(body)
		out = append(out, rendered{p.file, strings.TrimRight(b.String(), "\n") + "\n"})
	}
	body, err := settingsPage()
	if err != nil {
		return nil, err
	}
	return append(out, rendered{"openetld.md", body}), nil
}

func front(b *strings.Builder, title, description string) {
	fmt.Fprintf(b, "---\ntitle: %s\ndescription: %s\n---\n\n", title, description)
}

// code is s in code font.
func code(s string) string {
	if strings.Contains(s, "`") {
		return "`` " + s + " ``"
	}
	return "`" + s + "`"
}

// list joins items as prose: "a", "a or b", "a, b or c".
func list(items []string, conj string) string {
	switch len(items) {
	case 0:
		return ""
	case 1:
		return items[0]
	}
	return strings.Join(items[:len(items)-1], ", ") + " " + conj + " " + items[len(items)-1]
}

var codeSpan = regexp.MustCompile("`[^`]*`")

// text is plain text from a schema or a usage string as Markdown: it
// escapes what Markdown would read as emphasis or HTML, outside code spans,
// which the text may use.
func text(s string) string {
	s = strings.TrimSpace(s)
	var b strings.Builder
	last := 0
	for _, m := range codeSpan.FindAllStringIndex(s, -1) {
		b.WriteString(escape(s[last:m[0]]))
		b.WriteString(s[m[0]:m[1]])
		last = m[1]
	}
	b.WriteString(escape(s[last:]))
	return b.String()
}

var escaper = strings.NewReplacer(`\`, `\\`, `*`, `\*`, `_`, `\_`, `<`, `\<`)

func escape(s string) string { return escaper.Replace(s) }

// cell is s in a table's cell.
func cell(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "|", `\|`), "\n", " ")
}

var mdLink = regexp.MustCompile(`\[([^\]]*)\]\([^)]*\)`)

// slug is the anchor GitHub and the docs site give a heading: its text,
// lowercase, without punctuation, with spaces as hyphens.
func slug(heading string) string {
	t := mdLink.ReplaceAllString(heading, "$1")
	var b strings.Builder
	for _, r := range strings.ToLower(t) {
		switch {
		case r == ' ':
			b.WriteRune('-')
		case r == '-' || r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.Is(unicode.Mn, r):
			b.WriteRune(r)
		}
	}
	return b.String()
}
