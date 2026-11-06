package spark

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/openetl-io/openetl/internal/platform/errs"
)

// yarn submits with spark-submit, then finds, watches and kills through the
// ResourceManager's REST API, because spark-submit can't search by tag or
// kill on YARN.
type yarn struct {
	*submit
	rm   string
	http *http.Client
}

var yarnSubmitted = regexp.MustCompile(`Submitted application (application_\d+_\d+)`)

func (y *yarn) Kind() string { return "yarn" }

func (y *yarn) Submit(ctx context.Context, app Application) (string, error) {
	lines, err := y.run(ctx, submitArgs(y.master, app, map[string]string{
		"spark.yarn.tags":                     app.Name,
		"spark.yarn.submit.waitAppCompletion": "false",
	})...)
	if m := find(lines, yarnSubmitted); m != nil {
		return m[1], nil
	}
	if err == nil {
		err = errors.New("spark-submit didn't say which application it submitted: " + lastLines(lines, 3))
	}
	return "", failed(err, "spark: submit to YARN")
}

type yarnApp struct {
	ID          string `json:"id"`
	State       string `json:"state"`
	FinalStatus string `json:"finalStatus"`
	Diagnostics string `json:"diagnostics"`
	TrackingURL string `json:"trackingUrl"`
	StartedTime int64  `json:"startedTime"`
}

// Find returns the newest application tagged name. YARN keeps tags in
// lowercase, as Name makes them.
func (y *yarn) Find(ctx context.Context, name string) (string, error) {
	var res struct {
		Apps *struct {
			App []yarnApp `json:"app"`
		} `json:"apps"`
	}
	if err := y.call(ctx, http.MethodGet, "/ws/v1/cluster/apps?applicationTags="+url.QueryEscape(name), nil, &res); err != nil {
		return "", err
	}
	var newest yarnApp
	if res.Apps != nil {
		for _, a := range res.Apps.App {
			if a.StartedTime >= newest.StartedTime {
				newest = a
			}
		}
	}
	return newest.ID, nil
}

func (y *yarn) Status(ctx context.Context, id string) (State, error) {
	var res struct {
		App yarnApp `json:"app"`
	}
	if err := y.call(ctx, http.MethodGet, "/ws/v1/cluster/apps/"+url.PathEscape(id), nil, &res); err != nil {
		return State{}, err
	}
	a := res.App
	st := State{State: a.State, URL: a.TrackingURL}
	switch a.State {
	case "FINISHED", "FAILED", "KILLED":
		st.Done = true
		if a.FinalStatus != "SUCCEEDED" {
			st.Failed = true
			st.State = cmp.Or(a.FinalStatus, a.State)
			st.Message = "application " + id + " ended " + st.State
			if d := strings.TrimSpace(a.Diagnostics); d != "" {
				st.Message += ": " + d
			}
		}
	}
	return st, nil
}

func (y *yarn) Kill(ctx context.Context, id string) error {
	return y.call(ctx, http.MethodPut, "/ws/v1/cluster/apps/"+url.PathEscape(id)+"/state", map[string]string{"state": "KILLED"}, nil)
}

func (y *yarn) call(ctx context.Context, method, path string, body, out any) error {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, y.rm+path, r)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := y.http.Do(req)
	if err != nil {
		return errs.Wrap(errs.Transient, err, "spark: YARN's ResourceManager")
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	if err != nil {
		return errs.Wrap(errs.Transient, err, "spark: YARN's ResourceManager")
	}
	if res.StatusCode/100 != 2 {
		return errs.New(errs.Transient, "spark: YARN's ResourceManager: %s %s: %s: %.200s", method, path, res.Status, b)
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(b, out)
}

// resourceManager is the ResourceManager's web address, from the
// yarn-site.xml spark-submit reads, in YARN_CONF_DIR or HADOOP_CONF_DIR.
func resourceManager() (string, error) {
	dir := cmp.Or(os.Getenv("YARN_CONF_DIR"), os.Getenv("HADOOP_CONF_DIR"))
	if dir == "" {
		return "", errs.New(errs.ConfigInvalid, "spark: a YARN master needs HADOOP_CONF_DIR on the runner, as spark-submit does")
	}
	b, err := os.ReadFile(filepath.Join(dir, "yarn-site.xml")) //nolint:gosec // the operator's Hadoop configuration, as spark-submit reads it
	if err != nil {
		return "", errs.Wrap(errs.ConfigInvalid, err, "spark: YARN's settings")
	}
	var site struct {
		Properties []struct {
			Name  string `xml:"name"`
			Value string `xml:"value"`
		} `xml:"property"`
	}
	if err := xml.Unmarshal(b, &site); err != nil { //nolint:gosec // the operator's own file
		return "", errs.Wrap(errs.ConfigInvalid, err, "spark: yarn-site.xml")
	}
	props := map[string]string{}
	for _, p := range site.Properties {
		props[p.Name] = strings.TrimSpace(p.Value)
	}
	scheme, addr := "http", props["yarn.resourcemanager.webapp.address"]
	if props["yarn.http.policy"] == "HTTPS_ONLY" {
		scheme, addr = "https", props["yarn.resourcemanager.webapp.https.address"]
	}
	if addr == "" {
		host := props["yarn.resourcemanager.hostname"]
		if host == "" {
			return "", errs.New(errs.ConfigInvalid, "spark: yarn-site.xml names no ResourceManager")
		}
		addr = host + ":8088"
		if scheme == "https" {
			addr = host + ":8090"
		}
	}
	return fmt.Sprintf("%s://%s", scheme, addr), nil
}
