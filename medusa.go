// Package medusa is a zero-allocation distributed in-memory data grid: a small,
// Hazelcast-style cluster of nodes that together host partitioned, replicated
// maps and talk to each other over protobuf.
//
// A Node wires together three layers: a transport (framed protobuf messaging),
// cluster membership (which derives the partition table), and the imap service
// (the distributed maps). Inbound frames are dispatched by message type to the
// owning layer.
package medusa

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lodgvideon/medusa/cluster"
	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/imap"
	"github.com/lodgvideon/medusa/metrics"
	"github.com/lodgvideon/medusa/transport"
)

// defaultMaintenanceInterval is how often the background loop retries joining
// (while isolated) and gossips its membership view.
const defaultMaintenanceInterval = 3 * time.Second

// failureMissThreshold is how many consecutive missed heartbeats evict a peer.
// With the default interval this is ~9s of unresponsiveness before eviction.
const failureMissThreshold = 3

// tombstoneTTL is how long an evicted member's tombstone suppresses gossip
// re-adds before it is pruned.
const tombstoneTTL = 5 * time.Minute

// snapshotInterval is how often a node persists its store when a DataDir is set.
const snapshotInterval = 30 * time.Second

const snapshotFile = "snapshot.pb"

// Node is a single member of a Medusa cluster.
type Node struct {
	mem     *cluster.Membership
	maps    *imap.Service
	tr      transport.Transport
	seeds   []string
	log     *slog.Logger
	dataDir string

	maintCancel         context.CancelFunc
	maintDone           chan struct{}
	closeOnce           sync.Once
	lastMigratedVersion uint64
	lastSaveNano        int64
}

// Config configures a Node.
type Config struct {
	// ID is the unique node id. Defaults to Addr when empty.
	ID string
	// Addr is the address advertised to peers — what other nodes dial to reach
	// this one. It must be concrete (not host:0). In Kubernetes set this to the
	// pod's stable DNS name (e.g. medusa-0.medusa:7700) so the identity survives
	// restarts even though the pod IP changes.
	Addr string
	// BindAddr is the local listen address. Defaults to Addr. Set it to bind on
	// a different interface/name than the advertised Addr — e.g. bind ":7700"
	// while advertising a stable DNS name.
	BindAddr string
	// Seeds are cluster addresses the background maintenance loop joins through
	// while this node is isolated. Safe to include this node's own address.
	Seeds []string
	// MaintenanceInterval overrides how often the node retries joining and
	// gossips. Zero uses defaultMaintenanceInterval.
	MaintenanceInterval time.Duration
	// Logger receives structured logs. Zero uses slog.Default().
	Logger *slog.Logger
	// DataDir, when set, enables persistence: the node loads a snapshot from it
	// on start and writes one periodically and on graceful leave, so the cluster
	// survives a whole-cluster restart. Empty disables persistence.
	DataDir string
	// Transport overrides the default transport — tests inject an in-memory
	// transport here. When set, Addr should match the transport's address.
	Transport transport.Transport
}

// New creates and starts a Node: it builds the membership and map service and
// begins listening. Call Join afterwards to connect to an existing cluster.
func New(cfg Config) (*Node, error) {
	bindAddr := cfg.BindAddr
	if bindAddr == "" {
		bindAddr = cfg.Addr
	}
	tr := cfg.Transport
	if tr == nil {
		// Default network transport is the Poseidon HTTP/2 stack (h2c), which
		// handles values up to its advertised ~16 MiB stream window. For larger
		// values, inject transport.NewTCP via Transport. The transport binds to
		// bindAddr; peers reach this node at the advertised cfg.Addr.
		tr = transport.NewPoseidon(bindAddr)
	}
	id := cfg.ID
	if id == "" {
		id = cfg.Addr
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	n := &Node{tr: tr, seeds: cfg.Seeds, log: logger.With("node", id), dataDir: cfg.DataDir}
	n.mem = cluster.New(cluster.Member{ID: id, Addr: cfg.Addr}, tr)
	n.maps = imap.NewService(n.mem, tr)

	if n.dataDir != "" {
		if err := n.loadSnapshot(); err != nil {
			n.log.Warn("snapshot load failed", "err", err)
		} else {
			n.log.Info("snapshot loaded", "entries", n.maps.LocalEntryCount())
		}
		n.lastSaveNano = time.Now().UnixNano()
	}

	if err := tr.Listen(n.dispatch); err != nil {
		return nil, err
	}

	interval := cfg.MaintenanceInterval
	if interval <= 0 {
		interval = defaultMaintenanceInterval
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.maintCancel = cancel
	n.maintDone = make(chan struct{})
	go n.maintain(ctx, interval)

	return n, nil
}

// maintain keeps membership healthy: while isolated it retries joining the
// seeds (so nodes that start before their seeds eventually converge), and once
// it has peers it periodically gossips its view. This is what lets a cluster
// self-assemble under parallel startup (e.g. a Kubernetes StatefulSet).
func (n *Node) maintain(ctx context.Context, interval time.Duration) {
	defer close(n.maintDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if len(n.mem.Members()) <= 1 && len(n.seeds) > 0 {
				jctx, cancel := context.WithTimeout(ctx, interval)
				if err := n.mem.Join(jctx, n.seeds); err != nil {
					// Expected while bootstrapping (seeds not yet reachable); the
					// loop retries on the next tick.
					n.log.Debug("join attempt failed", "seeds", n.seeds, "err", err)
				}
				cancel()
			} else {
				n.mem.Gossip(ctx)
				// Evict peers that have gone silent (crashed or partitioned away)
				// so they stop owning partitions they can no longer serve.
				if evicted := n.mem.DetectFailures(ctx, failureMissThreshold); len(evicted) > 0 {
					metrics.Evicted.Add(int64(len(evicted)))
					n.log.Warn("evicted unresponsive peers", "peers", evicted)
				}
				n.mem.PruneTombstones(tombstoneTTL)
			}
			// When the partition table changes (a node joined, left, or was
			// evicted), move the data that this node no longer owns to its new
			// holders.
			if v := n.mem.Version(); v != n.lastMigratedVersion {
				n.maps.Migrate(ctx, n.mem.Table())
				n.lastMigratedVersion = v
				metrics.Migrations.Add(1)
				n.log.Info("rebalanced partitions", "version", v, "members", len(n.mem.Members()))
			}
			// Reclaim expired entries (lazy expiry already hides them on read).
			if swept := n.maps.SweepExpired(); swept > 0 {
				metrics.Swept.Add(int64(swept))
			}
			// Persist a snapshot at the configured interval.
			if n.dataDir != "" && time.Now().UnixNano()-n.lastSaveNano > int64(snapshotInterval) {
				if err := n.saveSnapshot(); err != nil {
					n.log.Warn("snapshot save failed", "err", err)
				}
				n.lastSaveNano = time.Now().UnixNano()
			}
		}
	}
}

func (n *Node) snapshotPath() string { return filepath.Join(n.dataDir, snapshotFile) }

// loadSnapshot restores this node's persisted state, if a snapshot exists.
func (n *Node) loadSnapshot() error {
	data, err := os.ReadFile(n.snapshotPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil // first start, nothing to load
		}
		return err
	}
	var snap medusav1.Snapshot
	if err := snap.UnmarshalVT(data); err != nil {
		return err
	}
	n.maps.Restore(&snap)
	return nil
}

// saveSnapshot writes this node's state to disk atomically (temp file + rename).
func (n *Node) saveSnapshot() error {
	if err := os.MkdirAll(n.dataDir, 0o755); err != nil {
		return err
	}
	data, err := n.maps.Snapshot().MarshalVT()
	if err != nil {
		return err
	}
	tmp := n.snapshotPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, n.snapshotPath())
}

// dispatch routes an inbound frame to the subsystem that owns its message type.
func (n *Node) dispatch(reqType medusav1.MessageType, req, respBuf []byte) (medusav1.MessageType, []byte, error) {
	switch reqType {
	case medusav1.MessageType_MESSAGE_TYPE_JOIN_REQUEST,
		medusav1.MessageType_MESSAGE_TYPE_MEMBER_LIST,
		medusav1.MessageType_MESSAGE_TYPE_HEARTBEAT,
		medusav1.MessageType_MESSAGE_TYPE_LEAVE:
		return n.mem.Handle(reqType, req, respBuf)
	case medusav1.MessageType_MESSAGE_TYPE_PUT_REQUEST,
		medusav1.MessageType_MESSAGE_TYPE_GET_REQUEST,
		medusav1.MessageType_MESSAGE_TYPE_REMOVE_REQUEST,
		medusav1.MessageType_MESSAGE_TYPE_EXECUTE_REQUEST:
		return n.maps.Handle(reqType, req, respBuf)
	default:
		return 0, respBuf, fmt.Errorf("medusa: unhandled message type %v", reqType)
	}
}

// Join connects this node to an existing cluster via the given seed addresses.
func (n *Node) Join(ctx context.Context, seeds []string) error {
	return n.mem.Join(ctx, seeds)
}

// Map returns a handle to the named distributed map.
func (n *Node) Map(name string) *imap.Map { return n.maps.Map(name) }

// LocalEntryCount returns how many map entries this node currently stores. It
// is a useful signal that data has migrated to a node and a building block for
// metrics.
func (n *Node) LocalEntryCount() int { return n.maps.LocalEntryCount() }

// Members returns the current cluster members, sorted by id.
func (n *Node) Members() []cluster.Member { return n.mem.Members() }

// Addr returns the node's transport address.
func (n *Node) Addr() string { return n.tr.Addr() }

// CheckLiveness pings peers and evicts unresponsive ones, returning evicted ids.
func (n *Node) CheckLiveness(ctx context.Context) []string {
	return n.mem.CheckLiveness(ctx)
}

// Leave gracefully removes this node from the cluster: it hands its partitions
// to their successors (per the membership without this node), announces its
// departure so peers rebalance immediately, then shuts down. Use it instead of
// Close for planned shutdowns — scale-down or rolling restart — to avoid the
// window where a partition's data lives only on its backup.
func (n *Node) Leave(ctx context.Context) error {
	selfID := n.mem.Self().ID
	n.maps.Migrate(ctx, n.mem.TableWithout(selfID))
	n.mem.AnnounceLeave(ctx)
	return n.Close()
}

// Close stops the maintenance loop and releases the transport. Safe to call
// more than once.
func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		if n.maintCancel != nil {
			n.maintCancel()
			<-n.maintDone
		}
		// Capture writes since the last periodic snapshot. For a whole-cluster
		// shutdown (where graceful migration has nowhere to go) this is what is
		// reloaded on restart.
		if n.dataDir != "" {
			if err := n.saveSnapshot(); err != nil {
				n.log.Warn("final snapshot save failed", "err", err)
			}
		}
	})
	return n.tr.Close()
}
