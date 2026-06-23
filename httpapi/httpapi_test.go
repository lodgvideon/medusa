package httpapi_test

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lodgvideon/medusa"
	"github.com/lodgvideon/medusa/httpapi"
	"github.com/lodgvideon/medusa/transport"
)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

// newTestServer spins up a single in-memory-transport node behind the HTTP API.
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	sw := transport.NewSwitch()
	node, err := medusa.New(medusa.Config{ID: "n1", Addr: "n1", Transport: sw.NewTransport("n1")})
	if err != nil {
		t.Fatalf("medusa.New: %v", err)
	}
	srv := httptest.NewServer(httpapi.New(node))
	t.Cleanup(func() {
		srv.Close()
		_ = node.Close()
	})
	return srv
}

func do(t *testing.T, method, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

func TestHealthAndReady(t *testing.T) {
	srv := newTestServer(t)
	for _, path := range []string{"/healthz", "/readyz"} {
		resp := do(t, http.MethodGet, srv.URL+path, "")
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s status = %d, want 200", path, resp.StatusCode)
		}
		resp.Body.Close()
	}
}

func TestPutGetDeleteRoundTrip(t *testing.T) {
	srv := newTestServer(t)
	url := srv.URL + "/v1/maps/users/alice"

	if resp := do(t, http.MethodPut, url, "active"); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status = %d, want 204", resp.StatusCode)
	}

	resp := do(t, http.MethodGet, url, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", resp.StatusCode)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(got) != "active" {
		t.Errorf("GET body = %q, want active", got)
	}

	if resp := do(t, http.MethodDelete, url, ""); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("DELETE status = %d, want 204", resp.StatusCode)
	}
	if resp := do(t, http.MethodGet, url, ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete status = %d, want 404", resp.StatusCode)
	}
}

func TestStats(t *testing.T) {
	srv := newTestServer(t)
	// Store one entry, then check it shows up in the stats.
	if resp := do(t, http.MethodPut, srv.URL+"/v1/maps/m/k", "v"); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT status = %d", resp.StatusCode)
	}
	resp := do(t, http.MethodGet, srv.URL+"/stats", "")
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	if !strings.Contains(s, "\"members\":1") {
		t.Errorf("stats = %s, want members:1", s)
	}
	if !strings.Contains(s, "\"localEntries\":1") {
		t.Errorf("stats = %s, want localEntries:1", s)
	}
	// A single-node test server still reports the configured replication factor;
	// the default is one backup.
	if !strings.Contains(s, "\"backups\":1") {
		t.Errorf("stats = %s, want backups:1", s)
	}
}

func TestPutWithTTLExpires(t *testing.T) {
	srv := newTestServer(t)
	url := srv.URL + "/v1/maps/m/ttlkey"

	if resp := do(t, http.MethodPut, url+"?ttl=80ms", "v"); resp.StatusCode != http.StatusNoContent {
		t.Fatalf("PUT?ttl status = %d", resp.StatusCode)
	}
	if resp := do(t, http.MethodGet, url, ""); resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		t.Fatalf("GET before expiry status = %d, want 200", resp.StatusCode)
	} else {
		resp.Body.Close()
	}

	time.Sleep(140 * time.Millisecond)
	resp := do(t, http.MethodGet, url, "")
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after expiry status = %d, want 404", resp.StatusCode)
	}
}

func TestPutWithBadTTLIs400(t *testing.T) {
	srv := newTestServer(t)
	resp := do(t, http.MethodPut, srv.URL+"/v1/maps/m/k?ttl=notaduration", "v")
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	srv := newTestServer(t)
	do(t, http.MethodPut, srv.URL+"/v1/maps/m/k", "v").Body.Close() // bump a counter

	resp := do(t, http.MethodGet, srv.URL+"/metrics", "")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	for _, want := range []string{"medusa_map_ops_total", "medusa_cluster_members 1", "medusa_map_entries"} {
		if !strings.Contains(s, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
}

func TestExecuteAppend(t *testing.T) {
	srv := newTestServer(t)
	execURL := srv.URL + "/v1/maps/m/k/execute?proc=append"

	do(t, http.MethodPost, execURL, "ab").Body.Close()
	resp := do(t, http.MethodPost, execURL, "cd")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "abcd" {
		t.Fatalf("execute append result = %q, want abcd", body)
	}

	g := do(t, http.MethodGet, srv.URL+"/v1/maps/m/k", "")
	gb, _ := io.ReadAll(g.Body)
	g.Body.Close()
	if string(gb) != "abcd" {
		t.Errorf("stored value = %q, want abcd", gb)
	}
}

func TestExecuteMissingProcIs400(t *testing.T) {
	srv := newTestServer(t)
	resp := do(t, http.MethodPost, srv.URL+"/v1/maps/m/k/execute", "x")
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestGetMissingKeyIs404(t *testing.T) {
	srv := newTestServer(t)
	resp := do(t, http.MethodGet, srv.URL+"/v1/maps/users/ghost", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
	resp.Body.Close()
}

func TestMembersJSON(t *testing.T) {
	srv := newTestServer(t)
	resp := do(t, http.MethodGet, srv.URL+"/members", "")
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "\"id\":\"n1\"") {
		t.Errorf("members body = %s, want it to contain node n1", body)
	}
}
