// Package docs checks docs/ against the rules in docs/AGENTS.md that a
// machine can check: frontmatter, headings, raw HTML, links and meta.json.
package docs

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"unicode"
)

const root = "../../docs"

// Files in docs/ that aren't pages.
var notPages = []string{"AGENTS.md", "CLAUDE.md"}

type page struct {
	path  string
	front map[string]string
	// Lines outside the frontmatter, with fenced code blanked.
	body []string
	// Each line's number in the file, for messages.
	lineNo []int
}

func TestPages(t *testing.T) {
	pages := load(t)
	if len(pages) == 0 {
		t.Fatal("no pages in docs/")
	}
	for _, p := range pages {
		for _, k := range []string{"title", "description"} {
			if p.front[k] == "" {
				t.Errorf("%s: frontmatter has no %s", p.path, k)
			}
		}
		for i, l := range p.body {
			at := fmt.Sprintf("%s:%d", p.path, p.lineNo[i])
			if strings.HasPrefix(l, "# ") {
				t.Errorf("%s: a # heading; the title is the page's heading, so start at ##", at)
			}
			if m := htmlTag.FindString(stripCode(l)); m != "" {
				t.Errorf("%s: raw HTML %s, which the site drops", at, m)
			}
			for _, m := range image.FindAllStringSubmatch(stripCode(l), -1) {
				if strings.TrimSpace(m[1]) == "" {
					t.Errorf("%s: image %s has no alt text", at, m[2])
				}
			}
		}
	}
}

func TestLinks(t *testing.T) {
	pages := load(t)
	anchors := map[string][]string{}
	for _, p := range pages {
		anchors[p.path] = headingIDs(p.body)
	}
	for _, p := range pages {
		for i, l := range p.body {
			for _, target := range links(l) {
				if err := checkLink(p.path, target, true, anchors); err != nil {
					t.Errorf("%s:%d: %s: %v", p.path, p.lineNo[i], target, err)
				}
			}
		}
	}
	// The repository's front pages link into docs/ too, as GitHub resolves
	// them; the site doesn't read them.
	for _, f := range []string{"../../README.md", "../../CONTRIBUTING.md"} {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := blankFences(strings.Split(string(b), "\n"))
		for i, l := range body {
			for _, target := range links(l) {
				if err := checkLink(f, target, false, anchors); err != nil {
					t.Errorf("%s:%d: %s: %v", f, i+1, target, err)
				}
			}
		}
	}
}

func TestMeta(t *testing.T) {
	err := filepath.WalkDir(root, func(dir string, d os.DirEntry, err error) error {
		if err != nil || !d.IsDir() || d.Name() == "images" {
			return err
		}
		b, err := os.ReadFile(filepath.Join(dir, "meta.json"))
		if err != nil {
			t.Errorf("%s: no meta.json", dir)
			return nil
		}
		var meta struct {
			Title       string   `json:"title"`
			Description string   `json:"description"`
			Icon        string   `json:"icon"`
			Root        *bool    `json:"root"`
			Pages       []string `json:"pages"`
		}
		if err := json.Unmarshal(b, &meta); err != nil {
			t.Errorf("%s/meta.json: %v", dir, err)
			return nil
		}
		if meta.Title == "" && dir != root {
			t.Errorf("%s/meta.json: no title", dir)
		}
		if meta.Root != nil {
			t.Errorf("%s/meta.json: has root, but sections stay folders in the site's sidebar", dir)
		}
		if filepath.Dir(dir) == root && (meta.Description == "" || meta.Icon == "") {
			t.Errorf("%s/meta.json: a section needs a description and an icon", dir)
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		var want []string
		for _, e := range entries {
			name := e.Name()
			switch {
			case e.IsDir() && name != "images":
				want = append(want, name)
			case strings.HasSuffix(name, ".md") && !slices.Contains(notPages, name):
				want = append(want, strings.TrimSuffix(name, ".md"))
			}
		}
		for _, w := range want {
			if !slices.Contains(meta.Pages, w) {
				t.Errorf("%s/meta.json: pages doesn't list %s, so the site leaves it out", dir, w)
			}
		}
		for _, p := range meta.Pages {
			if strings.HasPrefix(p, "---") || strings.HasPrefix(p, "[") || strings.HasPrefix(p, "external:") {
				continue
			}
			if !slices.Contains(want, p) {
				t.Errorf("%s/meta.json: pages lists %s, which isn't a page or folder here", dir, p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

var (
	// A tag or a comment. Autolinks such as <https://…> aren't HTML.
	htmlTag = regexp.MustCompile(`<!--|</?[A-Za-z][A-Za-z0-9-]*(\s[^<>]*)?/?>`)
	image   = regexp.MustCompile(`!\[([^\]]*)\]\(([^)\s]+)`)
	link    = regexp.MustCompile(`\]\(([^)\s]+)(\s+"[^"]*")?\)`)
	codeRun = regexp.MustCompile("`+[^`]*`+")
)

func load(t *testing.T) []page {
	t.Helper()
	var pages []page
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".md") || slices.Contains(notPages, d.Name()) {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		p, err := parse(path, string(b))
		if err != nil {
			t.Errorf("%s: %v", path, err)
			return nil
		}
		pages = append(pages, p)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return pages
}

func parse(path, s string) (page, error) {
	lines := strings.Split(s, "\n")
	p := page{path: path, front: map[string]string{}}
	if len(lines) == 0 || lines[0] != "---" {
		return p, fmt.Errorf("no frontmatter: the first line isn't ---")
	}
	end := slices.Index(lines[1:], "---")
	if end < 0 {
		return p, fmt.Errorf("frontmatter isn't closed with ---")
	}
	for _, l := range lines[1 : end+1] {
		k, v, ok := strings.Cut(l, ":")
		if ok && !strings.HasPrefix(l, " ") {
			p.front[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"'`)
		}
	}
	first := end + 2
	body, err := blankFences(lines[first:])
	if err != nil {
		return p, err
	}
	p.body = body
	for i := range body {
		p.lineNo = append(p.lineNo, first+i+1)
	}
	return p, nil
}

// blankFences blanks fenced code blocks' lines, fences included.
func blankFences(lines []string) ([]string, error) {
	out := make([]string, len(lines))
	fence := ""
	for i, l := range lines {
		trimmed := strings.TrimLeft(l, " ")
		switch {
		case fence == "" && (strings.HasPrefix(trimmed, "```") || strings.HasPrefix(trimmed, "~~~")):
			fence = trimmed[:3]
		case fence != "" && strings.HasPrefix(trimmed, fence):
			fence = ""
		case fence == "":
			out[i] = l
		}
	}
	if fence != "" {
		return nil, fmt.Errorf("a code block isn't closed")
	}
	return out, nil
}

func stripCode(l string) string { return codeRun.ReplaceAllString(l, "") }

func links(l string) []string {
	var out []string
	for _, m := range link.FindAllStringSubmatch(stripCode(l), -1) {
		out = append(out, m[1])
	}
	return out
}

// checkLink checks a relative link from the file at from: that it starts
// with ./ or ../ when strict, as the site resolves only those, that its
// target exists, and that a page's anchor is one of its headings.
func checkLink(from, target string, strict bool, anchors map[string][]string) error {
	if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") || strings.HasPrefix(target, "mailto:") {
		return nil
	}
	path, anchor, _ := strings.Cut(target, "#")
	file := from
	if path != "" {
		if strict && !strings.HasPrefix(path, "./") && !strings.HasPrefix(path, "../") {
			return fmt.Errorf("start a relative link with ./ or ../")
		}
		file = filepath.Join(filepath.Dir(from), path)
		if _, err := os.Stat(file); err != nil {
			return fmt.Errorf("no such file")
		}
	}
	if anchor == "" || !strings.HasSuffix(file, ".md") {
		return nil
	}
	ids, ok := anchors[filepath.Clean(file)]
	if !ok {
		return nil
	}
	if !slices.Contains(ids, anchor) {
		return fmt.Errorf("no heading has the anchor #%s", anchor)
	}
	return nil
}

// headingIDs are the anchors GitHub and the site give a page's headings.
func headingIDs(body []string) []string {
	var ids []string
	seen := map[string]int{}
	for _, l := range body {
		if !strings.HasPrefix(l, "#") {
			continue
		}
		text := strings.TrimSpace(strings.TrimLeft(l, "#"))
		id := slug(text)
		if n := seen[id]; n > 0 {
			seen[id]++
			id = fmt.Sprintf("%s-%d", id, n)
		} else {
			seen[id] = 1
		}
		ids = append(ids, id)
	}
	return ids
}

var mdLink = regexp.MustCompile(`\[([^\]]*)\]\([^)]*\)`)

// slug is github-slugger's: the heading's text, lowercase, without
// punctuation, with spaces as hyphens.
func slug(heading string) string {
	text := mdLink.ReplaceAllString(heading, "$1")
	var b strings.Builder
	for _, r := range strings.ToLower(text) {
		switch {
		case r == ' ':
			b.WriteRune('-')
		case r == '-' || r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.Is(unicode.Mn, r):
			b.WriteRune(r)
		}
	}
	return b.String()
}
