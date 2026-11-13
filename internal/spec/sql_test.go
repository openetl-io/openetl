package spec

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

func TestParseSQLTransform(t *testing.T) {
	header := "apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: shop}\n"
	doc, err := Parse([]byte(header + `spec:
  schedule: {every: 1h}
  source:
    ref: shop-db
    streams:
      - name: orders
        eventTime: created_at
        watermark: { delay: 5s }
        timestamps: nanosecond
        columns: { amount: { type: string } }
      - name: customers
  destination:
    ref: lake
    tables: { revenue_by_minute: revenue }
  postSync:
    - dbt: { project: shop-models }
  transform:
    passthrough: none
    sql: |
      INSERT INTO customers
      SELECT id, lower(email) AS email FROM customers;
`))
	if err != nil {
		t.Fatal(err)
	}
	p := doc.Pipeline
	sql := p.GetTransform().GetSql()
	orders := p.GetSource().GetStreams()[0]
	if sql.GetSql() != "INSERT INTO customers\nSELECT id, lower(email) AS email FROM customers;\n" || sql.GetPassthrough() != apiv1.Passthrough_PASSTHROUGH_NONE ||
		orders.GetEventTime() != "created_at" || orders.GetWatermark().GetDelay().AsDuration().Seconds() != 5 ||
		p.GetSource().GetStreams()[1].GetEventTime() != "" || p.GetSource().GetStreams()[1].GetWatermark() != nil ||
		p.GetDestination().GetTables()["revenue_by_minute"] != "revenue" {
		t.Errorf("pipeline = %v", p)
	}

	continuous := header + "spec:\n  mode: continuous\n  source: {ref: s, streams: [{name: orders, eventTime: at}]}\n  destination: {ref: d}\n"
	for name, c := range map[string]struct {
		doc  string
		want func(*apiv1.PipelineSpec) bool
	}{
		"continuous": {continuous + "  transform: {sql: 'INSERT INTO a SELECT * FROM orders'}\n", func(p *apiv1.PipelineSpec) bool {
			return p.GetTransform().GetSql().GetPassthrough() == apiv1.Passthrough_PASSTHROUGH_UNSPECIFIED && p.GetSource().GetStreams()[0].GetWatermark() == nil
		}},
		"passthrough all": {continuous + "  transform: {sql: 'SELECT 1', passthrough: all}\n", func(p *apiv1.PipelineSpec) bool {
			return p.GetTransform().GetSql().GetPassthrough() == apiv1.Passthrough_PASSTHROUGH_ALL
		}},
		"no delay": {strings.Replace(continuous, "eventTime: at}", "eventTime: at, watermark: {}}", 1) + "  transform: {sql: 'SELECT 1'}\n", func(p *apiv1.PipelineSpec) bool {
			w := p.GetSource().GetStreams()[0].GetWatermark()
			return w != nil && w.GetDelay() == nil
		}},
		"zero delay": {strings.Replace(continuous, "eventTime: at}", "eventTime: at, watermark: {delay: 0s}}", 1) + "  transform: {sql: 'SELECT 1'}\n", func(p *apiv1.PipelineSpec) bool {
			return p.GetSource().GetStreams()[0].GetWatermark().GetDelay().AsDuration() == 0
		}},
	} {
		d, err := Parse([]byte(c.doc))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if !c.want(d.Pipeline) {
			t.Errorf("%s: %v", name, d.Pipeline)
		}
	}
}

func TestParseSQLTransformErrors(t *testing.T) {
	header := "apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\n"
	digest := strings.Repeat("ab", 32)
	spark := "spark: {cluster: lab, code: {jar: 's3://j/a.jar', sha256: " + digest + ", class: A}}"
	flink := "flink: {cluster: fl, code: {jar: 's3://j/a.jar', sha256: " + digest + "}}"
	scheduled := func(streams, transform string) string {
		return header + "spec:\n  schedule: {every: 1m}\n  source: {ref: s, streams: [" + streams + "]}\n  destination: {ref: d}\n" + transform
	}
	sql := func(streams string) string { return scheduled(streams, "  transform: {sql: 'SELECT 1'}\n") }
	for name, c := range map[string]struct{ doc, want string }{
		"sql and spark":         {scheduled("{name: a}", "  transform: {sql: 'SELECT 1', "+spark+"}\n"), "transform needs one of sql, spark and flink"},
		"sql and flink":         {strings.Replace(scheduled("{name: a}", "  transform: {sql: 'SELECT 1', "+flink+"}\n"), "schedule: {every: 1m}", "mode: continuous", 1), "transform needs one of sql, spark and flink"},
		"only passthrough":      {scheduled("{name: a}", "  transform: {passthrough: none}\n"), "transform needs one of"},
		"spark passthrough":     {scheduled("{name: a}", "  transform: {passthrough: none, "+spark+"}\n"), "transform.passthrough"},
		"passthrough value":     {scheduled("{name: a}", "  transform: {sql: 'SELECT 1', passthrough: some}\n"), "transform.passthrough"},
		"passthrough case":      {scheduled("{name: a}", "  transform: {sql: 'SELECT 1', passthrough: NONE}\n"), "transform.passthrough"},
		"empty":                 {scheduled("{name: a}", "  transform: {sql: ''}\n"), "transform.sql is empty"},
		"blank":                 {scheduled("{name: a}", "  transform: {sql: \"  \\n\\t\"}\n"), "transform.sql is empty"},
		"null":                  {scheduled("{name: a}", "  transform: {sql: }\n"), "transform.sql is empty"},
		"number":                {scheduled("{name: a}", "  transform: {sql: 5}\n"), "transform.sql: give its statements"},
		"list":                  {scheduled("{name: a}", "  transform: {sql: [SELECT 1]}\n"), "transform.sql: give its statements"},
		"file":                  {scheduled("{name: a}", "  transform: {sql: {file: shop.sql}}\n"), "transform.sql.file: openetl apply and openetl plan read"},
		"empty file":            {scheduled("{name: a}", "  transform: {sql: {file: ''}}\n"), "transform.sql.file is empty"},
		"other key":             {scheduled("{name: a}", "  transform: {sql: {path: shop.sql}}\n"), "transform.sql:"},
		"file and more":         {scheduled("{name: a}", "  transform: {sql: {file: shop.sql, text: x}}\n"), "transform.sql:"},
		"eventTime without sql": {scheduled("{name: a, eventTime: at}", ""), "source.streams[0].eventTime"},
		"eventTime with spark":  {scheduled("{name: a, eventTime: at}", "  transform: {"+spark+"}\n"), "source.streams[0].eventTime"},
		"watermark without sql": {scheduled("{name: a, eventTime: at, watermark: {delay: 1s}}", ""), "source.streams[0].eventTime"},
		"only a watermark":      {scheduled("{name: a, watermark: {delay: 1s}}", ""), "source.streams[0].watermark"},
		"no eventTime":          {sql("{name: a}, {name: b, watermark: {delay: 1s}}"), "source.streams[1].watermark needs source.streams[1].eventTime"},
		"delay":                 {sql("{name: a, eventTime: at, watermark: {delay: soon}}"), "source.streams[0].watermark.delay"},
		"negative delay":        {sql("{name: a, eventTime: at, watermark: {delay: -5s}}"), "source.streams[0].watermark.delay must be zero or more"},
		"watermark field":       {sql("{name: a, eventTime: at, watermark: {lag: 5s}}"), "lag"},
		"watermark text":        {sql("{name: a, eventTime: at, watermark: 5s}"), "watermark"},
		"collapse":              {sql("{name: a, collapse: true}"), "source.streams[0].collapse"},
		"typeChanges":           {sql("{name: a, typeChanges: allow}"), "source.streams[0].typeChanges"},
		"held":                  {sql("{name: a, held: last}"), "source.streams[0]: a transform's inputs can't be held"},
		"cells":                 {sql("{name: a, quarantine: cell}"), "source.streams[0]: a transform's inputs can't be held"},
		"groups":                {sql("{name: a, groups: [[x, y]]}"), "source.streams[0]: a transform's inputs can't be held"},
		"column held":           {sql("{name: a, columns: {x: {held: null}}}"), "source.streams[0]: a transform's inputs can't be held"},
		"flinkAccess":           {sql("{name: a}") + "  flinkAccess: {emrRole: 'arn:aws:iam::123456789012:role/r'}\n", "flinkAccess"},
		"sparkAccess":           {sql("{name: a}") + "  sparkAccess: {emrRole: 'arn:aws:iam::123456789012:role/r'}\n", "sparkAccess"},
		"reserved table":        {strings.Replace(sql("{name: a}"), "{ref: d}", "{ref: d, tables: {out: _openetl}}", 1), "its own records"},
		"one table":             {strings.Replace(sql("{name: a}"), "{ref: d}", "{ref: d, tables: {x: t, y: t}}", 1), "same table"},
	} {
		if _, err := Parse([]byte(c.doc)); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v, want %q", name, err, c.want)
		}
	}
}

// The SQL a version stores is the text as written, through the API's JSON
// and its digest's encoding.
func TestSQLTransformRoundTrip(t *testing.T) {
	text := "INSERT INTO \"Out\" SELECT 'a: b', \"x\" -- é\n\tFROM orders;  \n\nCREATE VIEW v AS SELECT 1"
	quoted, err := protojson.Marshal(&apiv1.SqlTransform{Sql: text})
	if err != nil {
		t.Fatal(err)
	}
	doc, err := Parse([]byte("apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\nspec:\n  mode: continuous\n" +
		"  source: {ref: s, streams: [{name: orders, eventTime: at, watermark: {delay: 1m30s}}]}\n  destination: {ref: d}\n" +
		"  transform: " + string(quoted) + "\n"))
	if err != nil {
		t.Fatal(err)
	}
	if got := doc.Pipeline.GetTransform().GetSql().GetSql(); got != text {
		t.Fatalf("sql = %q, want %q", got, text)
	}
	b, err := protojson.Marshal(doc.Pipeline)
	if err != nil {
		t.Fatal(err)
	}
	var stored apiv1.PipelineSpec
	if err := protojson.Unmarshal(b, &stored); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(&stored, doc.Pipeline) {
		t.Errorf("stored %v, want %v", &stored, doc.Pipeline)
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(doc.Pipeline)
	if err != nil {
		t.Fatal(err)
	}
	var back apiv1.PipelineSpec
	if err := proto.Unmarshal(wire, &back); err != nil || back.GetTransform().GetSql().GetSql() != text ||
		back.GetSource().GetStreams()[0].GetWatermark().GetDelay().AsDuration().Seconds() != 90 {
		t.Errorf("back = %v, %v", &back, err)
	}
}
