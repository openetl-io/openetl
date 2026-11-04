// Package spec parses resource documents (apiVersion, kind, metadata, spec)
// into api/v1 types. YAML spells enums in lowercase (merge), durations as Go
// durations (15m) and sizes with units (256MB).
package spec

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
)

const APIVersion = "openetl.io/v1alpha1"

const (
	KindSource      = "Source"
	KindDestination = "Destination"
	KindPipeline    = "Pipeline"
	// Connector versions: an external connector's descriptor, and a
	// declarative source's manifest.
	KindConnector         = "Connector"
	KindDeclarativeSource = "DeclarativeSource"
	// A dbt project's files, for post-sync steps.
	KindDbtProject = "DbtProject"
	// Where a workspace's platform events go.
	KindWebhook = "Webhook"
	// Where Spark applications run.
	KindSparkCluster = "SparkCluster"
	// Where Flink jobs run.
	KindFlinkCluster = "FlinkCluster"
)

// Document is one parsed definition. Connector is set for sources and
// destinations, Pipeline for pipelines, Descriptor for connectors, Manifest
// for declarative sources, Bundle for dbt projects, Webhook for webhooks,
// SparkCluster for Spark clusters, and FlinkCluster for Flink clusters.
type Document struct {
	APIVersion string
	Kind       string
	Name       string
	Workspace  string
	// Connector versions only: a semantic version.
	Version string
	// Connector versions only.
	Info       Info
	Connector  *apiv1.ConnectorConfig
	Pipeline   *apiv1.PipelineSpec
	Descriptor *Descriptor
	// The whole manifest, as canonical JSON, for declarative.Load.
	Manifest []byte
	// A gzipped tar of the project's files, which dbt.Canonical checks.
	Bundle       []byte
	Webhook      *apiv1.WebhookSpec
	SparkCluster *apiv1.SparkClusterSpec
	FlinkCluster *apiv1.FlinkClusterSpec
}

type envelope struct {
	APIVersion string    `yaml:"apiVersion"`
	Kind       string    `yaml:"kind"`
	Metadata   metadata  `yaml:"metadata"`
	Spec       yaml.Node `yaml:"spec"`
}

type metadata struct {
	Name        string       `yaml:"name"`
	Workspace   string       `yaml:"workspace"`
	Version     string       `yaml:"version"`
	Description string       `yaml:"description"`
	License     string       `yaml:"license"`
	Repository  string       `yaml:"repository"`
	Docs        string       `yaml:"docs"`
	Author      string       `yaml:"author"`
	Logo        string       `yaml:"logo"`
	Tags        []string     `yaml:"tags"`
	Release     *releaseYAML `yaml:"release"`
}

func (m metadata) hasInfo() bool {
	return m.Description != "" || m.License != "" || m.Repository != "" || m.Docs != "" || m.Author != "" || m.Logo != "" || m.Tags != nil || m.Release != nil
}

type releaseYAML struct {
	Tag string `yaml:"tag,omitempty" json:"tag,omitempty"`
	URL string `yaml:"url,omitempty" json:"url,omitempty"`
}

// Info describes a connector version for people and the UI. Only
// Repository is required, and only of a v1alpha2 descriptor.
type Info struct {
	Description string
	// An SPDX expression.
	License string
	// An https URL of the source repository. A package's name stays with it.
	Repository string
	// An https URL.
	Docs   string
	Author string
	// An https URL of an image.
	Logo       string
	Tags       []string
	ReleaseTag string
	// An https URL, such as the release's page.
	ReleaseURL string
}

var (
	tagPattern     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)
	licensePattern = regexp.MustCompile(`^[A-Za-z0-9.+:()\- ]{1,100}$`)
)

// CheckInfo checks a connector version's metadata.
func CheckInfo(info Info) error {
	switch {
	case len(info.Author) > 200:
		return errors.New("metadata.author is longer than 200 characters")
	case len(info.Description) > 500 || strings.ContainsAny(info.Description, "\n\r"):
		return errors.New("metadata.description must be one line of at most 500 characters")
	case info.License != "" && !licensePattern.MatchString(info.License):
		return fmt.Errorf("metadata.license %q isn't an SPDX expression, such as Apache-2.0", info.License)
	case len(info.Tags) > 20:
		return errors.New("metadata.tags has more than 20 tags")
	case len(info.ReleaseTag) > 255 || strings.ContainsAny(info.ReleaseTag, " \t\n"):
		return fmt.Errorf("metadata.release.tag %q isn't a git tag", info.ReleaseTag)
	}
	for _, t := range info.Tags {
		if !tagPattern.MatchString(t) {
			return fmt.Errorf("metadata.tags: %q must be lowercase letters, digits and hyphens, at most 32 characters", t)
		}
	}
	for _, f := range []struct{ field, v string }{
		{"metadata.repository", info.Repository}, {"metadata.docs", info.Docs},
		{"metadata.logo", info.Logo}, {"metadata.release.url", info.ReleaseURL},
	} {
		if f.v != "" && !httpURL(f.v, false) {
			return fmt.Errorf("%s %q must be an https URL", f.field, f.v)
		}
	}
	return nil
}

func parseInfo(m metadata) (Info, error) {
	info := Info{Description: m.Description, License: m.License, Repository: m.Repository, Docs: m.Docs, Author: m.Author, Logo: m.Logo, Tags: m.Tags}
	if m.Release != nil {
		info.ReleaseTag, info.ReleaseURL = m.Release.Tag, m.Release.URL
	}
	return info, CheckInfo(info)
}

var namePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)

// ValidName reports whether name can be a resource ID: lowercase letters,
// digits and hyphens, at most 63 characters.
func ValidName(name string) bool { return namePattern.MatchString(name) }

// Parse parses one YAML or JSON document.
func Parse(content []byte) (*Document, error) {
	var env envelope
	if err := strict(content, &env); err != nil {
		return nil, err
	}
	versioned := env.Kind == KindConnector || env.Kind == KindDeclarativeSource
	switch {
	case env.APIVersion != APIVersion && (env.Kind != KindConnector || env.APIVersion != APIVersionConnector):
		want := APIVersion
		if env.Kind == KindConnector {
			want = APIVersionConnector
		}
		return nil, fmt.Errorf("apiVersion is %q, want %q", env.APIVersion, want)
	case versioned && !ValidPackageName(env.Metadata.Name):
		return nil, fmt.Errorf("metadata.name %q must be owner/name or name, each lowercase letters, digits and hyphens, at most 63 characters", env.Metadata.Name)
	case !versioned && !ValidName(env.Metadata.Name):
		return nil, fmt.Errorf("metadata.name %q must be lowercase letters, digits and hyphens, at most 63 characters", env.Metadata.Name)
	case env.Metadata.Workspace != "" && !ValidName(env.Metadata.Workspace):
		return nil, fmt.Errorf("metadata.workspace %q isn't a valid name", env.Metadata.Workspace)
	case env.Spec.Kind == 0:
		return nil, errors.New("spec is missing")
	}
	doc := &Document{APIVersion: env.APIVersion, Kind: env.Kind, Name: env.Metadata.Name, Workspace: env.Metadata.Workspace, Version: env.Metadata.Version}
	if versioned {
		doc.Name = FullName(env.Metadata.Name)
	}
	switch {
	case versioned && env.Metadata.Workspace != "":
		return nil, errors.New("metadata.workspace: connectors belong to the org, not a workspace")
	case versioned && !semverPattern.MatchString(env.Metadata.Version):
		return nil, fmt.Errorf("metadata.version %q must be a semantic version, such as 1.2.0", env.Metadata.Version)
	case !versioned && env.Metadata.Version != "":
		return nil, fmt.Errorf("metadata.version: a %s has no version", env.Kind)
	case !versioned && env.Metadata.hasInfo():
		return nil, fmt.Errorf("metadata: author, logo, release and the like are a connector version's, not a %s's", env.Kind)
	case env.APIVersion == APIVersionConnector && env.Metadata.Repository == "":
		return nil, errors.New("metadata.repository, the package's source repository, is missing")
	}
	var err error
	if doc.Info, err = parseInfo(env.Metadata); err != nil {
		return nil, err
	}
	spec, err := yaml.Marshal(&env.Spec)
	if err != nil {
		return nil, err
	}
	switch env.Kind {
	case KindSource, KindDestination:
		doc.Connector, err = parseConnector(spec)
	case KindPipeline:
		doc.Pipeline, err = parsePipeline(spec)
	case KindConnector:
		if env.APIVersion == APIVersionConnector {
			doc.Descriptor, err = parseDescriptor(spec)
		} else {
			doc.Descriptor, err = parseV1alpha1Descriptor(spec)
		}
	case KindDeclarativeSource:
		// declarative.Load checks the rest.
		doc.Manifest, err = canonicalJSON(content)
	case KindDbtProject:
		doc.Bundle, err = parseDbtProject(spec)
	case KindWebhook:
		doc.Webhook, err = parseWebhook(spec)
	case KindSparkCluster:
		doc.SparkCluster, err = parseSparkCluster(spec)
	case KindFlinkCluster:
		doc.FlinkCluster, err = parseFlinkCluster(spec)
	default:
		return nil, fmt.Errorf("kind %q isn't one of Source, Destination, Pipeline, Connector, DeclarativeSource, DbtProject, Webhook, SparkCluster or FlinkCluster", env.Kind)
	}
	if err != nil {
		return nil, fmt.Errorf("spec: %w", err)
	}
	return doc, nil
}

func strict(content []byte, v any) error {
	dec := yaml.NewDecoder(bytes.NewReader(content))
	dec.KnownFields(true)
	if err := dec.Decode(v); err != nil {
		return err
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); err == nil {
		return errors.New("one document per definition; split files on ---")
	}
	return nil
}

type connectorYAML struct {
	Connector string         `yaml:"connector"`
	Config    map[string]any `yaml:"config"`
}

func parseConnector(b []byte) (*apiv1.ConnectorConfig, error) {
	var c connectorYAML
	if err := strict(b, &c); err != nil {
		return nil, err
	}
	if c.Connector == "" {
		return nil, errors.New("connector is missing")
	}
	cfg, err := structpb.NewStruct(c.Config)
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	return &apiv1.ConnectorConfig{Connector: c.Connector, Config: cfg}, nil
}

type pipelineYAML struct {
	Mode      string        `yaml:"mode"`
	Schedule  *scheduleYAML `yaml:"schedule"`
	DataPlane string        `yaml:"dataPlane"`
	Source    struct {
		Ref     string       `yaml:"ref"`
		Streams []streamYAML `yaml:"streams"`
	} `yaml:"source"`
	Destination struct {
		Ref       string `yaml:"ref"`
		Write     string `yaml:"write"`
		Namespace string `yaml:"namespace"`
		Naming    string `yaml:"naming"`
		Deletes   string `yaml:"deletes"`
	} `yaml:"destination"`
	SchemaPolicy string        `yaml:"schemaPolicy"`
	Delivery     *deliveryYAML `yaml:"delivery"`
	Types        *struct {
		Infinity string `yaml:"infinity"`
	} `yaml:"types"`
	PostSync []postSyncYAML `yaml:"postSync"`
	Heal     *struct {
		Mode string `yaml:"mode"`
	} `yaml:"heal"`
	Transform *struct {
		Spark *sparkTransformYAML `yaml:"spark"`
		Flink *flinkTransformYAML `yaml:"flink"`
	} `yaml:"transform"`
	SparkAccess *sparkAccessYAML `yaml:"sparkAccess"`
	FlinkAccess *sparkAccessYAML `yaml:"flinkAccess"`
}

type postSyncYAML struct {
	Dbt *struct {
		Project string `yaml:"project"`
		Command string `yaml:"command"`
		Select  string `yaml:"select"`
		Exclude string `yaml:"exclude"`
		Schema  string `yaml:"schema"`
	} `yaml:"dbt"`
	Spark *sparkStepYAML `yaml:"spark"`
}

type scheduleYAML struct {
	Every  string `yaml:"every"`
	Cron   string `yaml:"cron"`
	Jitter string `yaml:"jitter"`
}

type streamYAML struct {
	Name       string `yaml:"name"`
	Sync       string `yaml:"sync"`
	Cursor     string `yaml:"cursor"`
	Timestamps string `yaml:"timestamps"`
	Require    string `yaml:"require"`
	Collapse   *bool  `yaml:"collapse"`
	Columns    map[string]struct {
		Op   string `yaml:"op"`
		Type string `yaml:"type"`
	} `yaml:"columns"`
}

type deliveryYAML struct {
	Require string `yaml:"require"`
	Commit  *struct {
		Interval string `yaml:"interval"`
		MaxBytes string `yaml:"maxBytes"`
		Scope    string `yaml:"scope"`
	} `yaml:"commit"`
	ResumePointTimeout string `yaml:"resumePointTimeout"`
}

func parseSchedule(y *scheduleYAML) (*apiv1.Schedule, error) {
	sc := &apiv1.Schedule{}
	if y.Every != "" {
		d, err := duration("schedule.every", y.Every)
		if err != nil {
			return nil, err
		}
		if d.AsDuration() <= 0 {
			return nil, errors.New("schedule.every must be positive")
		}
		sc.Trigger = &apiv1.Schedule_Every{Every: d}
	} else {
		sc.Trigger = &apiv1.Schedule_Cron{Cron: y.Cron}
	}
	if y.Jitter != "" {
		var err error
		if sc.Jitter, err = duration("schedule.jitter", y.Jitter); err != nil {
			return nil, err
		}
	}
	return sc, nil
}

func parsePipeline(b []byte) (*apiv1.PipelineSpec, error) {
	var y pipelineYAML
	if err := strict(b, &y); err != nil {
		return nil, err
	}
	p := &apiv1.PipelineSpec{DataPlane: y.DataPlane}
	var err error
	if p.Mode, err = enum(apiv1.PipelineMode_value, "PIPELINE_MODE_", "mode", y.Mode, apiv1.PipelineMode_PIPELINE_MODE_SCHEDULED); err != nil {
		return nil, err
	}
	continuous := p.Mode == apiv1.PipelineMode_PIPELINE_MODE_CONTINUOUS
	switch {
	case continuous && y.Schedule != nil:
		return nil, errors.New("schedule: a continuous pipeline runs all the time, so it has no schedule")
	case continuous && len(y.PostSync) > 0:
		return nil, errors.New("postSync: steps run after a sync, and a continuous pipeline has no syncs")
	case !continuous && (y.Schedule == nil || (y.Schedule.Every == "") == (y.Schedule.Cron == "")):
		return nil, errors.New("schedule needs one of every or cron")
	case y.Transform != nil && (y.Transform.Spark == nil) == (y.Transform.Flink == nil):
		return nil, errors.New("transform needs one of spark and flink")
	case y.Transform != nil && y.Transform.Spark != nil && continuous:
		return nil, errors.New("transform.spark: a Spark transform runs in a scheduled pipeline's syncs, and a continuous pipeline has none")
	case y.Transform != nil && y.Transform.Flink != nil && !continuous:
		return nil, errors.New("transform.flink: a Flink transform runs in a continuous pipeline, and this one is scheduled")
	}
	spark := y.Transform != nil && y.Transform.Spark != nil
	flink := y.Transform != nil && y.Transform.Flink != nil
	if y.Schedule != nil {
		if p.Schedule, err = parseSchedule(y.Schedule); err != nil {
			return nil, err
		}
	}

	if y.Source.Ref == "" || y.Destination.Ref == "" {
		return nil, errors.New("source.ref and destination.ref are required")
	}
	p.Source = &apiv1.PipelineSource{Ref: y.Source.Ref}
	seen := map[string]bool{}
	for i, s := range y.Source.Streams {
		if s.Name == "" || seen[s.Name] {
			return nil, fmt.Errorf("source.streams[%d]: missing or repeated name %q", i, s.Name)
		}
		seen[s.Name] = true
		st := &apiv1.PipelineStream{Name: s.Name, Cursor: s.Cursor, Collapse: s.Collapse}
		if st.Sync, err = enum(apiv1.SyncMode_value, "SYNC_MODE_", "sync", s.Sync, apiv1.SyncMode_SYNC_MODE_UNSPECIFIED); err != nil {
			return nil, fmt.Errorf("source.streams[%d]: %w", i, err)
		}
		if st.Timestamps, err = enum(apiv1.TimestampUnit_value, "TIMESTAMP_UNIT_", "timestamps", s.Timestamps, apiv1.TimestampUnit_TIMESTAMP_UNIT_UNSPECIFIED); err != nil {
			return nil, fmt.Errorf("source.streams[%d]: %w", i, err)
		}
		switch {
		case spark && st.Timestamps == apiv1.TimestampUnit_TIMESTAMP_UNIT_NANOSECOND:
			return nil, fmt.Errorf("source.streams[%d].timestamps: Spark keeps only microseconds, so a Spark transform's inputs can't keep nanoseconds", i)
		case (spark || flink) && s.Collapse != nil:
			return nil, fmt.Errorf("source.streams[%d].collapse: a transform's inputs aren't written to the destination, and its outputs aren't collapsed", i)
		}
		if st.Require, err = enum(apiv1.DeliveryLevel_value, "DELIVERY_LEVEL_", "require", s.Require, apiv1.DeliveryLevel_DELIVERY_LEVEL_UNSPECIFIED); err != nil {
			return nil, fmt.Errorf("source.streams[%d]: %w", i, err)
		}
		for name, c := range s.Columns {
			field := fmt.Sprintf("source.streams[%d].columns.%s", i, name)
			if c.Op != "" {
				return nil, fmt.Errorf("%s: column ops aren't supported yet", field)
			}
			col := &apiv1.PipelineColumn{}
			if col.Type, err = enum(apiv1.ColumnType_value, "COLUMN_TYPE_", field+".type", c.Type, apiv1.ColumnType_COLUMN_TYPE_UNSPECIFIED); err != nil {
				return nil, err
			}
			if col.Type == apiv1.ColumnType_COLUMN_TYPE_UNSPECIFIED {
				return nil, fmt.Errorf("%s: needs a type", field)
			}
			if st.Columns == nil {
				st.Columns = map[string]*apiv1.PipelineColumn{}
			}
			st.Columns[name] = col
		}
		p.Source.Streams = append(p.Source.Streams, st)
	}
	p.Destination = &apiv1.PipelineDestination{Ref: y.Destination.Ref, Namespace: y.Destination.Namespace}
	if p.Destination.Write, err = enum(apiv1.WriteMode_value, "WRITE_MODE_", "destination.write", y.Destination.Write, apiv1.WriteMode_WRITE_MODE_UNSPECIFIED); err != nil {
		return nil, err
	}
	if p.Destination.Naming, err = enum(apiv1.Naming_value, "NAMING_", "destination.naming", y.Destination.Naming, apiv1.Naming_NAMING_UNSPECIFIED); err != nil {
		return nil, err
	}
	if p.Destination.Deletes, err = enum(apiv1.DeleteMode_value, "DELETE_MODE_", "destination.deletes", y.Destination.Deletes, apiv1.DeleteMode_DELETE_MODE_UNSPECIFIED); err != nil {
		return nil, err
	}
	if p.SchemaPolicy, err = enum(apiv1.SchemaPolicy_value, "SCHEMA_POLICY_", "schemaPolicy", y.SchemaPolicy, apiv1.SchemaPolicy_SCHEMA_POLICY_ALLOW_ALL); err != nil {
		return nil, err
	}
	if y.Delivery != nil {
		if p.Delivery, err = parseDelivery(y.Delivery); err != nil {
			return nil, err
		}
	}
	if y.Types != nil {
		p.Types = &apiv1.Types{}
		if p.Types.Infinity, err = enum(apiv1.Infinity_value, "INFINITY_", "types.infinity", y.Types.Infinity, apiv1.Infinity_INFINITY_UNSPECIFIED); err != nil {
			return nil, err
		}
	}
	if y.Heal != nil {
		p.Heal = &apiv1.Heal{}
		if p.Heal.Mode, err = enum(apiv1.HealMode_value, "HEAL_MODE_", "heal.mode", y.Heal.Mode, apiv1.HealMode_HEAL_MODE_UNSPECIFIED); err != nil {
			return nil, err
		}
	}
	if spark {
		t, err := parseSparkTransform(y.Transform.Spark)
		if err != nil {
			return nil, err
		}
		p.Transform = &apiv1.Transform{Engine: &apiv1.Transform_Spark{Spark: t}}
	}
	if flink {
		t, err := parseFlinkTransform(y.Transform.Flink)
		if err != nil {
			return nil, err
		}
		p.Transform = &apiv1.Transform{Engine: &apiv1.Transform_Flink{Flink: t}}
	}
	for i, s := range y.PostSync {
		field := fmt.Sprintf("postSync[%d]", i)
		switch {
		case (s.Dbt == nil) == (s.Spark == nil):
			return nil, fmt.Errorf("%s: needs one of dbt and spark", field)
		case s.Spark != nil:
			step, err := parseSparkStep(field+".spark", s.Spark)
			if err != nil {
				return nil, err
			}
			p.PostSync = append(p.PostSync, &apiv1.PostSyncStep{Step: &apiv1.PostSyncStep_Spark{Spark: step}})
			continue
		}
		if !ValidName(s.Dbt.Project) {
			return nil, fmt.Errorf("%s.dbt.project: %q isn't a DbtProject name", field, s.Dbt.Project)
		}
		step := &apiv1.DbtStep{Project: s.Dbt.Project, Select: s.Dbt.Select, Exclude: s.Dbt.Exclude, Schema: s.Dbt.Schema}
		if step.Command, err = enum(apiv1.DbtCommand_value, "DBT_COMMAND_", field+".dbt.command", s.Dbt.Command, apiv1.DbtCommand_DBT_COMMAND_BUILD); err != nil {
			return nil, err
		}
		p.PostSync = append(p.PostSync, &apiv1.PostSyncStep{Step: &apiv1.PostSyncStep_Dbt{Dbt: step}})
	}
	if y.SparkAccess != nil {
		if !spark && !slices.ContainsFunc(p.PostSync, func(s *apiv1.PostSyncStep) bool { return s.GetSpark() != nil }) {
			return nil, errors.New("sparkAccess: the pipeline runs no Spark transform or step")
		}
		if p.SparkAccess, err = parseSparkAccess(y.SparkAccess); err != nil {
			return nil, err
		}
	}
	if y.FlinkAccess != nil {
		if !flink {
			return nil, errors.New("flinkAccess: the pipeline runs no Flink transform")
		}
		if p.FlinkAccess, err = parseFlinkAccess(y.FlinkAccess); err != nil {
			return nil, err
		}
	}
	return p, nil
}

type dbtProjectYAML struct {
	Bundle string `yaml:"bundle"`
	Path   string `yaml:"path"`
}

func parseDbtProject(b []byte) ([]byte, error) {
	var y dbtProjectYAML
	if err := strict(b, &y); err != nil {
		return nil, err
	}
	switch {
	case y.Path != "":
		return nil, errors.New("path is read by openetl apply, which sends the project's files as bundle in its place")
	case y.Bundle == "":
		return nil, errors.New("bundle is missing: the project's files as a base64 gzipped tar, which openetl apply makes from path")
	}
	bundle, err := base64.StdEncoding.DecodeString(y.Bundle)
	if err != nil {
		return nil, fmt.Errorf("bundle isn't base64: %w", err)
	}
	return bundle, nil
}

type webhookYAML struct {
	URL       string   `yaml:"url"`
	Events    []string `yaml:"events"`
	Pipelines []string `yaml:"pipelines"`
	Secrets   []string `yaml:"secrets"`
}

// maxWebhookSecrets is enough for a key and the one it replaces, and one
// more.
const maxWebhookSecrets = 3

func parseWebhook(b []byte) (*apiv1.WebhookSpec, error) {
	var y webhookYAML
	if err := strict(b, &y); err != nil {
		return nil, err
	}
	u, err := url.Parse(y.URL)
	switch {
	case y.URL == "":
		return nil, errors.New("url is missing")
	case err != nil || u.Host == "" || u.Scheme != "http" && u.Scheme != "https":
		return nil, fmt.Errorf("url %q isn't an http or https URL", y.URL)
	case u.User != nil:
		return nil, errors.New("url: put credentials in a signing secret, not the URL")
	case len(y.Events) == 0:
		return nil, errors.New("events is missing: the event types to send, or globs such as sync.*")
	case len(y.Secrets) == 0:
		return nil, errors.New("secrets is missing: a secret holding a whsec_ key signs each delivery; set one with openetl secret set NAME")
	case len(y.Secrets) > maxWebhookSecrets:
		return nil, fmt.Errorf("secrets: at most %d", maxWebhookSecrets)
	}
	for _, names := range [][]string{y.Pipelines, y.Secrets} {
		for _, n := range names {
			if !ValidName(n) {
				return nil, fmt.Errorf("%q isn't a valid name", n)
			}
		}
	}
	if len(slices.Compact(slices.Sorted(slices.Values(y.Secrets)))) != len(y.Secrets) {
		return nil, errors.New("secrets: each secret once")
	}
	return &apiv1.WebhookSpec{Url: y.URL, Events: y.Events, Pipelines: y.Pipelines, Secrets: y.Secrets}, nil
}

func parseDelivery(y *deliveryYAML) (*apiv1.Delivery, error) {
	d := &apiv1.Delivery{}
	var err error
	if d.Require, err = enum(apiv1.DeliveryLevel_value, "DELIVERY_LEVEL_", "delivery.require", y.Require, apiv1.DeliveryLevel_DELIVERY_LEVEL_UNSPECIFIED); err != nil {
		return nil, err
	}
	switch y.ResumePointTimeout {
	case "":
	case "none":
		d.ResumePointTimeout = durationpb.New(0)
	default:
		if d.ResumePointTimeout, err = duration("delivery.resumePointTimeout", y.ResumePointTimeout); err != nil {
			return nil, err
		}
		if d.ResumePointTimeout.AsDuration() <= 0 {
			return nil, errors.New("delivery.resumePointTimeout must be positive, or none")
		}
	}
	if c := y.Commit; c != nil {
		d.Commit = &apiv1.Commit{}
		if c.Interval != "" {
			if d.Commit.Interval, err = duration("delivery.commit.interval", c.Interval); err != nil {
				return nil, err
			}
			if d.Commit.Interval.AsDuration() <= 0 {
				return nil, errors.New("delivery.commit.interval must be positive")
			}
		}
		if c.MaxBytes != "" {
			if d.Commit.MaxBytes, err = Size(c.MaxBytes); err != nil {
				return nil, fmt.Errorf("delivery.commit.maxBytes: %w", err)
			}
		}
		if d.Commit.Scope, err = enum(apiv1.CommitScope_value, "COMMIT_SCOPE_", "delivery.commit.scope", c.Scope, apiv1.CommitScope_COMMIT_SCOPE_UNSPECIFIED); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// enum maps a lowercase value such as "full_refresh" to its proto enum. An
// empty value gives def.
func enum[E ~int32](values map[string]int32, prefix, field, v string, def E) (E, error) {
	if v == "" {
		return def, nil
	}
	n, ok := values[prefix+strings.ToUpper(v)]
	if !ok || n == 0 || strings.ToLower(v) != v {
		var names []string
		for name, n := range values {
			if n != 0 {
				names = append(names, strings.ToLower(strings.TrimPrefix(name, prefix)))
			}
		}
		slices.Sort(names)
		return def, fmt.Errorf("%s: %q isn't one of %s", field, v, strings.Join(names, ", "))
	}
	return E(n), nil
}

func duration(field, v string) (*durationpb.Duration, error) {
	d, err := time.ParseDuration(v)
	if err != nil {
		return nil, fmt.Errorf("%s: %q isn't a duration such as 90s or 15m", field, v)
	}
	return durationpb.New(d), nil
}

var units = []struct {
	suffix string
	n      int64
}{
	{"KiB", 1 << 10}, {"MiB", 1 << 20}, {"GiB", 1 << 30}, {"TiB", 1 << 40},
	{"KB", 1e3}, {"MB", 1e6}, {"GB", 1e9}, {"TB", 1e12}, {"B", 1},
}

// Size parses a byte count such as 256MB (10^6), 64MiB (2^20) or 1024.
func Size(v string) (int64, error) {
	if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
		return n, nil
	}
	for _, u := range units {
		if num, ok := strings.CutSuffix(v, u.suffix); ok {
			n, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
			if err != nil || n <= 0 || n > (1<<62)/u.n {
				break
			}
			return n * u.n, nil
		}
	}
	return 0, fmt.Errorf("%q isn't a size such as 256MB or 64MiB", v)
}
