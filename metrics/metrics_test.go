package metrics_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/lodgvideon/medusa/metrics"
)

func TestWriteProm(t *testing.T) {
	metrics.PutOps.Store(7)
	metrics.GetOps.Store(3)
	metrics.Swept.Store(2)
	metrics.Reconciled.Store(5)

	var buf bytes.Buffer
	metrics.WriteProm(&buf, metrics.Gauges{Members: 3, LocalEntries: 42})
	out := buf.String()

	for _, want := range []string{
		"# TYPE medusa_map_ops_total counter",
		`medusa_map_ops_total{op="put"} 7`,
		`medusa_map_ops_total{op="get"} 3`,
		"medusa_entries_swept_total 2",
		"# TYPE medusa_cluster_members gauge",
		"medusa_cluster_members 3",
		"medusa_map_entries 42",
		"medusa_migrations_total",
		"medusa_members_evicted_total",
		"medusa_entries_reconciled_total 5",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q\n---\n%s", want, out)
		}
	}
}
