package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// recordWriter writes what a read gets.
type recordWriter interface {
	rows(stream string, rows [][]byte) error
	records(stream string, rec arrow.RecordBatch) error
	close() error
}

func newRecordWriter(format, path string, stdout io.Writer) (recordWriter, error) {
	out := stdout
	var file *os.File
	if path != "" && format != "parquet" {
		var err error
		if file, err = os.Create(filepath.Clean(path)); err != nil {
			return nil, err
		}
		out = file
	}
	switch format {
	case "table":
		return &tableWriter{out: out, file: file, streams: map[string]*table{}}, nil
	case "jsonl":
		return &jsonlWriter{out: out, file: file}, nil
	case "parquet":
		if path == "" {
			return nil, errors.New("--format parquet writes a file per stream into the directory --out names")
		}
		if err := os.MkdirAll(path, 0o750); err != nil {
			return nil, err
		}
		dir, err := os.OpenRoot(path)
		if err != nil {
			return nil, err
		}
		return &parquetWriter{dir: dir, files: map[string]*parquetFile{}}, nil
	}
	return nil, fmt.Errorf("--format %s: want table, jsonl or parquet", format)
}

// recordRows are rec's rows as JSON objects.
func recordRows(rec arrow.RecordBatch) ([][]byte, error) {
	var buf bytes.Buffer
	if err := array.RecordToJSON(rec, &buf); err != nil {
		return nil, err
	}
	var rows [][]byte
	for line := range bytes.Lines(buf.Bytes()) {
		rows = append(rows, bytes.TrimSpace(line))
	}
	return rows, nil
}

type jsonlWriter struct {
	out  io.Writer
	file *os.File
}

func (j *jsonlWriter) rows(stream string, rows [][]byte) error {
	name, _ := json.Marshal(stream)
	var buf bytes.Buffer
	for _, r := range rows {
		buf.WriteString(`{"stream":`)
		buf.Write(name)
		buf.WriteString(`,"record":`)
		buf.Write(r)
		buf.WriteString("}\n")
	}
	_, err := j.out.Write(buf.Bytes())
	return err
}

func (j *jsonlWriter) records(stream string, rec arrow.RecordBatch) error {
	rows, err := recordRows(rec)
	if err != nil {
		return err
	}
	return j.rows(stream, rows)
}

func (j *jsonlWriter) close() error {
	if j.file != nil {
		return j.file.Close()
	}
	return nil
}

// tableWriter prints each stream's records as a table at the end.
type tableWriter struct {
	out     io.Writer
	file    *os.File
	streams map[string]*table
	order   []string
}

type table struct {
	cols []string
	rows []map[string]string
}

const maxCell = 40

func (tw *tableWriter) table(stream string) *table {
	t := tw.streams[stream]
	if t == nil {
		t = &table{}
		tw.streams[stream] = t
		tw.order = append(tw.order, stream)
	}
	return t
}

func (tw *tableWriter) rows(stream string, rows [][]byte) error {
	t := tw.table(stream)
	for _, r := range rows {
		for _, k := range objectKeys(r) {
			if !slices.Contains(t.cols, k) {
				t.cols = append(t.cols, k)
			}
		}
		dec := json.NewDecoder(bytes.NewReader(r))
		dec.UseNumber()
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			return fmt.Errorf("stream %s: a record isn't a JSON object: %w", stream, err)
		}
		cells := map[string]string{}
		for k, v := range m {
			cells[k] = cell(v)
		}
		t.rows = append(t.rows, cells)
	}
	return nil
}

func (tw *tableWriter) records(stream string, rec arrow.RecordBatch) error {
	t := tw.table(stream)
	for _, f := range rec.Schema().Fields() {
		if !slices.Contains(t.cols, f.Name) {
			t.cols = append(t.cols, f.Name)
		}
	}
	rows, err := recordRows(rec)
	if err != nil {
		return err
	}
	return tw.rows(stream, rows)
}

func (tw *tableWriter) close() error {
	for i, name := range tw.order {
		t := tw.streams[name]
		if i > 0 {
			fmt.Fprintln(tw.out)
		}
		fmt.Fprintf(tw.out, "%s (%d records)\n", name, len(t.rows))
		w := tabwriter.NewWriter(tw.out, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, strings.Join(t.cols, "\t"))
		for _, r := range t.rows {
			cells := make([]string, len(t.cols))
			for i, c := range t.cols {
				cells[i] = r[c]
			}
			fmt.Fprintln(w, strings.Join(cells, "\t"))
		}
		if err := w.Flush(); err != nil {
			return err
		}
	}
	if tw.file != nil {
		return tw.file.Close()
	}
	return nil
}

func cell(v any) string {
	var s string
	switch x := v.(type) {
	case nil:
		s = ""
	case string:
		s = x
	case json.Number:
		s = x.String()
	default:
		b, _ := json.Marshal(x)
		s = string(b)
	}
	s = strings.NewReplacer("\t", " ", "\n", " ", "\r", " ").Replace(s)
	if r := []rune(s); len(r) > maxCell {
		s = string(r[:maxCell-1]) + "…"
	}
	return s
}

// objectKeys are a JSON object's keys, in order.
func objectKeys(b []byte) []string {
	dec := json.NewDecoder(bytes.NewReader(b))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return nil
	}
	var keys []string
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			break
		}
		k, _ := t.(string)
		keys = append(keys, k)
		var skip json.RawMessage
		if dec.Decode(&skip) != nil {
			break
		}
	}
	return keys
}

// parquetWriter writes a file per stream, and another when a stream's
// schema changes.
type parquetWriter struct {
	dir   *os.Root
	files map[string]*parquetFile
}

type parquetFile struct {
	n      int
	schema *arrow.Schema
	f      *os.File
	w      *pqarrow.FileWriter
}

var unsafeName = regexp.MustCompile(`[^A-Za-z0-9._-]`)

func (pw *parquetWriter) rows(stream string, _ [][]byte) error {
	return fmt.Errorf("the records of stream %s are JSON, whose types the platform's engine infers; read it as a table or JSON lines", stream)
}

func (pw *parquetWriter) records(stream string, rec arrow.RecordBatch) error {
	pf := pw.files[stream]
	if pf == nil || !pf.schema.Equal(rec.Schema()) {
		n := 1
		if pf != nil {
			if err := pf.close(); err != nil {
				return err
			}
			n = pf.n + 1
		}
		name := unsafeName.ReplaceAllString(stream, "_")
		if n > 1 {
			name += fmt.Sprintf(".%d", n)
		}
		f, err := pw.dir.Create(name + ".parquet")
		if err != nil {
			return err
		}
		w, err := pqarrow.NewFileWriter(rec.Schema(), f, parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Zstd)), pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
		if err != nil {
			_ = f.Close()
			return fmt.Errorf("stream %s: %w", stream, err)
		}
		pf = &parquetFile{n: n, schema: rec.Schema(), f: f, w: w}
		pw.files[stream] = pf
	}
	return pf.w.WriteBuffered(rec)
}

// close writes the footer; the writer closes the file.
func (pf *parquetFile) close() error {
	err := pf.w.Close()
	_ = pf.f.Close()
	return err
}

func (pw *parquetWriter) close() error {
	var errs []error
	for _, pf := range pw.files {
		errs = append(errs, pf.close())
	}
	errs = append(errs, pw.dir.Close())
	return errors.Join(errs...)
}
