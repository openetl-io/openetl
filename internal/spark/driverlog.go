package spark

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8sclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/openetl-io/openetl/internal/platform/logging"
)

// How much of a failed driver's log reaches the sync log: its last lines,
// each cut to driverLogLine, within driverLogBytes in all. driverLogRead
// bounds what the API sends, and is far larger because the API cuts the end
// of its answer, which holds the lines worth having.
const (
	driverLogLines = 100
	driverLogBytes = 64 << 10
	driverLogLine  = 16 << 10
	driverLogRead  = 8 << 20
	driverLogWait  = 30 * time.Second
	redacted       = "[redacted]"
)

// driverLogger is a Submitter that can read a failed driver's log.
type driverLogger interface {
	logDriver(ctx context.Context, id string, app Application)
}

// LogDriver writes the last lines of a failed application's driver to the
// submitter's log, where the runner can read them. It's best effort, and
// says so in one line if it can't, so it never changes how a step ends.
func LogDriver(ctx context.Context, sub Submitter, id string, app Application) {
	if d, ok := sub.(driverLogger); ok {
		d.logDriver(ctx, id, app)
	}
}

func (k *kubernetes) logDriver(ctx context.Context, id string, app Application) {
	ns, pod, ok := strings.Cut(id, ":")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, driverLogWait)
	defer cancel()
	lines, err := k.driverLines(ctx, ns, pod, app.Conf["spark.kubernetes.driver.podTemplateContainerName"])
	if err != nil {
		k.log.Warn("the failed driver's log couldn't be read, so its last lines may be missing here", "submission", id, "err", err)
	}
	if len(lines) == 0 {
		return
	}
	k.log.Warn(fmt.Sprintf("the failed driver's last %d lines", len(lines)), "submission", id)
	scrub := redactor(app.Conf)
	for _, l := range lines {
		k.log.Info(scrub(l), "component", "spark-driver")
	}
}

// driverLines returns the end of the pod's log. An empty container is the
// pod's only one. With an error, it returns what it read before it.
func (k *kubernetes) driverLines(ctx context.Context, ns, pod, container string) ([]string, error) {
	client, err := k.client()
	if err != nil {
		return nil, err
	}
	tail, limit := int64(driverLogLines), int64(driverLogRead)
	r, err := client.CoreV1().Pods(ns).GetLogs(pod, &corev1.PodLogOptions{Container: container, TailLines: &tail, LimitBytes: &limit}).Stream(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	var lines []string
	size := 0
	sc := logging.Lines(r)
	for sc.Scan() {
		l := sc.Text()
		if len(l) > driverLogLine {
			l = l[:driverLogLine] + "... (cut)"
		}
		lines = append(lines, l)
		size += len(l)
		for size > driverLogBytes || len(lines) > driverLogLines {
			size -= len(lines[0])
			lines = lines[1:]
		}
	}
	return lines, sc.Err()
}

// secretProperty is the default of spark.redaction.regex: the properties
// Spark keeps out of its own logs.
var secretProperty = regexp.MustCompile(`(?i)secret|password|token|access[.]key`)

// redactor masks the values of the application's secret properties, since a
// driver's log holds what the user's code printed and the sync log has
// readers the cluster's logs don't. Values under 8 bytes are left: masking
// "true" would only garble the lines.
func redactor(conf map[string]string) func(string) string {
	var known []string
	for k, v := range conf {
		if len(v) >= 8 && secretProperty.MatchString(k) {
			known = append(known, v)
		}
	}
	// Longest first, so a value that contains another goes whole.
	slices.SortFunc(known, func(a, b string) int { return len(b) - len(a) })
	return func(s string) string {
		for _, v := range known {
			s = strings.ReplaceAll(s, v, redacted)
		}
		return s
	}
}

// apiClient opens the Kubernetes API for reading a driver's log.
type apiClient func() (k8sclient.Interface, error)

// driverClient opens the API with what spark-submit finds on the runner: a
// kubeconfig, or the pod's service account. A SparkCluster's author chooses
// its master, so it refuses unless those credentials are for that API
// server, and never sends them anywhere else.
func driverClient(master string, conf map[string]string) apiClient {
	return func() (k8sclient.Interface, error) {
		loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(clientcmd.NewDefaultClientConfigLoadingRules(),
			&clientcmd.ConfigOverrides{CurrentContext: conf["spark.kubernetes.context"]})
		cfg, err := loader.ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("this runner has no Kubernetes credentials of its own: %w", err)
		}
		if !sameAPIServer(cfg.Host, strings.TrimPrefix(master, "k8s://")) {
			return nil, fmt.Errorf("this runner's Kubernetes credentials are for %s, not %s", cfg.Host, master)
		}
		return k8sclient.NewForConfig(cfg)
	}
}

// serviceNames are the API server's names inside its cluster, where a
// service account's credentials name its address instead.
var serviceNames = []string{"kubernetes", "kubernetes.default", "kubernetes.default.svc", "kubernetes.default.svc.cluster.local"}

type apiServer struct{ host, port, path string }

func parseAPIServer(s string) (apiServer, bool) {
	if !strings.Contains(s, "://") {
		s = "https://" + s
	}
	u, err := url.Parse(s)
	if err != nil || u.Hostname() == "" {
		return apiServer{}, false
	}
	port := u.Port()
	if port == "" {
		port = map[string]string{"http": "80", "https": "443"}[u.Scheme]
	}
	return apiServer{strings.ToLower(u.Hostname()), port, strings.TrimSuffix(u.Path, "/")}, true
}

// sameAPIServer reports whether credentials, the address a kubeconfig or
// service account names, is the API server master names.
func sameAPIServer(credentials, master string) bool {
	c, ok := parseAPIServer(credentials)
	m, ok2 := parseAPIServer(master)
	if !ok || !ok2 {
		return false
	}
	if c == m {
		return true
	}
	inside := strings.ToLower(os.Getenv("KUBERNETES_SERVICE_HOST"))
	return c.host == inside && c.port == m.port && c.path == m.path && slices.Contains(serviceNames, m.host)
}
