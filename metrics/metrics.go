// Package metrics holds process-wide counters for a medusa node and renders
// them in the Prometheus text exposition format. Counters are plain atomics —
// incrementing one is allocation-free, so it is cheap enough for the map hot
// path. Gauges (members, entries) are sampled at scrape time and passed to
// WriteProm by the caller.
package metrics

import (
	"fmt"
	"io"
	"sync/atomic"
)

// Counters incremented across the codebase.
var (
	PutOps         atomic.Int64 // map Put operations issued through this node
	GetOps         atomic.Int64 // map Get operations
	RemoveOps      atomic.Int64 // map Remove operations
	ExecuteOps     atomic.Int64 // EntryProcessor executions
	Swept          atomic.Int64 // entries reclaimed by TTL expiry
	Evicted        atomic.Int64 // peers removed by failure detection
	Migrations     atomic.Int64 // partition-migration passes run
	Reconciled     atomic.Int64 // entries re-pushed to backups by anti-entropy
	EvictedEntries atomic.Int64 // entries removed to enforce the max-size cap
	EventsEmitted  atomic.Int64 // entry events delivered to listeners
	EventsDropped  atomic.Int64 // entry events dropped because the listener queue was full
)

// Gauges are the point-in-time values sampled when /metrics is scraped.
type Gauges struct {
	Members      int
	LocalEntries int
}

// WriteProm renders all metrics to w in the Prometheus text format.
func WriteProm(w io.Writer, g Gauges) {
	counter := func(name, help string, labelled ...[2]any) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
		for _, lv := range labelled {
			fmt.Fprintf(w, "%s{op=%q} %d\n", name, lv[0], lv[1])
		}
	}
	gauge := func(name, help string, v int) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, help, name, name, v)
	}

	counter("medusa_map_ops_total", "Map operations by type.",
		[2]any{"put", PutOps.Load()},
		[2]any{"get", GetOps.Load()},
		[2]any{"remove", RemoveOps.Load()},
		[2]any{"execute", ExecuteOps.Load()},
	)
	fmt.Fprintf(w, "# HELP medusa_entries_swept_total Entries reclaimed by TTL expiry.\n")
	fmt.Fprintf(w, "# TYPE medusa_entries_swept_total counter\nmedusa_entries_swept_total %d\n", Swept.Load())
	fmt.Fprintf(w, "# HELP medusa_members_evicted_total Peers removed by failure detection.\n")
	fmt.Fprintf(w, "# TYPE medusa_members_evicted_total counter\nmedusa_members_evicted_total %d\n", Evicted.Load())
	fmt.Fprintf(w, "# HELP medusa_migrations_total Partition-migration passes run.\n")
	fmt.Fprintf(w, "# TYPE medusa_migrations_total counter\nmedusa_migrations_total %d\n", Migrations.Load())
	fmt.Fprintf(w, "# HELP medusa_entries_reconciled_total Entries re-pushed to backups by anti-entropy.\n")
	fmt.Fprintf(w, "# TYPE medusa_entries_reconciled_total counter\nmedusa_entries_reconciled_total %d\n", Reconciled.Load())
	fmt.Fprintf(w, "# HELP medusa_entries_evicted_total Entries removed to enforce the max-size cap.\n")
	fmt.Fprintf(w, "# TYPE medusa_entries_evicted_total counter\nmedusa_entries_evicted_total %d\n", EvictedEntries.Load())
	fmt.Fprintf(w, "# HELP medusa_events_emitted_total Entry events delivered to listeners.\n")
	fmt.Fprintf(w, "# TYPE medusa_events_emitted_total counter\nmedusa_events_emitted_total %d\n", EventsEmitted.Load())
	fmt.Fprintf(w, "# HELP medusa_events_dropped_total Entry events dropped because the listener queue was full.\n")
	fmt.Fprintf(w, "# TYPE medusa_events_dropped_total counter\nmedusa_events_dropped_total %d\n", EventsDropped.Load())

	gauge("medusa_cluster_members", "Current number of cluster members.", g.Members)
	gauge("medusa_map_entries", "Live entries stored on this node.", g.LocalEntries)
}
