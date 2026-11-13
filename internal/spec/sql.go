package spec

import (
	"errors"
	"fmt"
	"strings"

	"go.yaml.in/yaml/v3"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

// parseSQLTransform reads transform.sql, the statements as text, and
// transform.passthrough. A file's path is the CLI's to read: a spec that
// still names one is refused, so a version always holds the SQL itself.
func parseSQLTransform(n *yaml.Node, passthrough string) (*apiv1.SqlTransform, error) {
	t := &apiv1.SqlTransform{}
	switch {
	case n.Kind == yaml.ScalarNode && n.ShortTag() == "!!str":
		if strings.TrimSpace(n.Value) == "" {
			return nil, errors.New("transform.sql is empty: give its INSERT INTO … SELECT statements, or { file: PATH }")
		}
		t.Sql = n.Value
	case n.Kind == yaml.ScalarNode && n.ShortTag() == "!!null":
		return nil, errors.New("transform.sql is empty: give its INSERT INTO … SELECT statements, or { file: PATH }")
	case n.Kind == yaml.MappingNode:
		var y struct {
			File string `yaml:"file"`
		}
		b, err := yaml.Marshal(n)
		if err != nil {
			return nil, fmt.Errorf("transform.sql: %w", err)
		}
		if err := strict(b, &y); err != nil {
			return nil, fmt.Errorf("transform.sql: %w", err)
		}
		if y.File == "" {
			return nil, errors.New("transform.sql.file is empty: give the path of a file of SQL, relative to the file that names it")
		}
		return nil, fmt.Errorf("transform.sql.file: openetl apply and openetl plan read %q and send its text as transform.sql; elsewhere, give the SQL itself", y.File)
	default:
		return nil, errors.New("transform.sql: give its statements as text, or { file: PATH }")
	}
	var err error
	if t.Passthrough, err = enum(apiv1.Passthrough_value, "PASSTHROUGH_", "transform.passthrough", passthrough, apiv1.Passthrough_PASSTHROUGH_UNSPECIFIED); err != nil {
		return nil, err
	}
	return t, nil
}

type watermarkYAML struct {
	Delay string `yaml:"delay"`
}

// parseEventTime reads a stream's eventTime and watermark, which only a SQL
// transform takes. at is the stream's field, source.streams[i].
func parseEventTime(at string, s streamYAML, sql bool, st *apiv1.PipelineStream) error {
	switch {
	case s.EventTime == "" && s.Watermark == nil:
		return nil
	case !sql && s.EventTime != "":
		return fmt.Errorf("%s.eventTime: event time is for a SQL transform's statements, and the pipeline has no transform.sql", at)
	case !sql:
		return fmt.Errorf("%s.watermark: a watermark is for a SQL transform's statements, and the pipeline has no transform.sql", at)
	case s.EventTime == "":
		return fmt.Errorf("%s.watermark needs %s.eventTime, the column whose time it follows", at, at)
	}
	st.EventTime = s.EventTime
	if s.Watermark == nil {
		return nil
	}
	st.Watermark = &apiv1.Watermark{}
	if s.Watermark.Delay != "" {
		d, err := duration(at+".watermark.delay", s.Watermark.Delay)
		if err != nil {
			return err
		}
		if d.AsDuration() < 0 {
			return fmt.Errorf("%s.watermark.delay must be zero or more", at)
		}
		st.Watermark.Delay = d
	}
	return nil
}
