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
	"crypto/tls"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lodgvideon/medusa/cluster"
	"github.com/lodgvideon/medusa/discovery"
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

// antiEntropyBatch is how many partitions each maintenance tick re-pushes to
// their backups. The loop rotates through all 271 partitions, so backups
// converge to the owner within roughly Count/antiEntropyBatch ticks.
const antiEntropyBatch = 8

// evictBatch caps how many over-cap entries a single maintenance tick evicts, so
// a large overshoot drains over several ticks instead of stalling the loop in one
// long pass of replicated removes.
const evictBatch = 1024

const snapshotFile = "snapshot.pb"

// walFile is the write-ahead log, replayed on top of the snapshot at startup and
// truncated at each snapshot checkpoint.
const walFile = "wal.log"

// Node is a single member of a Medusa cluster.
type Node struct {
	mem        *cluster.Membership
	maps       *imap.Service
	tr         transport.Transport
	disco      discovery.Discoverer
	log        *slog.Logger
	dataDir    string
	maxEntries int // soft per-node entry cap; 0 = unbounded (no eviction)

	maintCancel         context.CancelFunc
	maintDone           chan struct{}
	closeOnce           sync.Once
	lastMigratedVersion uint64
	lastSaveNano        int64
	checkpointDue       bool // a rebalance dropped partitions (or a checkpoint failed): checkpoint ASAP
	reconcileCursor     int  // next partition for the rotating anti-entropy pass
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
	// Ignored when Discovery is set; otherwise it backs a static Discoverer.
	Seeds []string
	// Discovery finds the peer addresses the maintenance loop joins through while
	// this node is isolated. Zero defaults to discovery.Static(Seeds) — the
	// classic fixed seed list. Set discovery.NewDNS(host, port) to resolve peers
	// from a name (e.g. a Kubernetes headless Service) so the cluster
	// self-assembles and scales without a hand-maintained seed list.
	Discovery discovery.Discoverer
	// Backups is the number of backup copies kept for every partition (the
	// replication factor minus one). Each write is synchronously replicated to
	// this many distinct peers, so the cluster tolerates that many simultaneous
	// holder failures without data loss. Zero selects the default of 1 (the
	// floor: medusa's "a single node failure loses no data" guarantee needs at
	// least one backup). A value larger than the number of peers is capped to
	// however many distinct backups the cluster can supply.
	Backups int
	// MaintenanceInterval overrides how often the node retries joining and
	// gossips. Zero uses defaultMaintenanceInterval.
	MaintenanceInterval time.Duration
	// MaxEntries caps how many live entries this node holds before it starts
	// evicting (a soft, per-node bound to prevent unbounded memory growth). Zero
	// means unbounded (no eviction — the default). When over the cap, the
	// maintenance loop removes a batch of this node's OWNED entries (a roughly
	// random selection), replicating each removal so backups stay consistent. It
	// is a cache-style bound: eviction deletes the entries cluster-wide, so enable
	// it only for maps that tolerate losing entries under pressure.
	MaxEntries int
	// Logger receives structured logs. Zero uses slog.Default().
	Logger *slog.Logger
	// DataDir, when set, enables persistence: the node loads a snapshot from it
	// on start and writes one periodically and on graceful leave, so the cluster
	// survives a whole-cluster restart. Empty disables persistence.
	DataDir string
	// TLS, when set, secures the inter-node transport with HTTP/2 over TLS
	// (ALPN "h2") instead of cleartext h2c. The same config is used both when
	// this node serves peers and when it dials them, so for mutual TLS it should
	// carry Certificates (presented as server and client cert), RootCAs (to
	// verify peers it dials) and ClientCAs + ClientAuth (to verify peers that
	// dial in). Ignored when Transport is set. Advertised Addr must match a name
	// in the certificate the peer presents.
	TLS *tls.Config
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
		// Default network transport is the Poseidon HTTP/2 stack, which handles
		// values up to its advertised ~16 MiB stream window. For larger values,
		// inject transport.NewTCP via Transport. The transport binds to bindAddr;
		// peers reach this node at the advertised cfg.Addr. A TLS config upgrades
		// the data plane from cleartext h2c to HTTP/2 over TLS.
		if cfg.TLS != nil {
			tr = transport.NewPoseidonTLS(bindAddr, cfg.TLS)
		} else {
			tr = transport.NewPoseidon(bindAddr)
		}
	}
	id := cfg.ID
	if id == "" {
		id = cfg.Addr
	}

	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	backups := cfg.Backups
	if backups < 1 {
		backups = 1 // floor: at least one backup so a single failure loses no data
	}
	disco := cfg.Discovery
	if disco == nil {
		disco = discovery.Static(cfg.Seeds)
	}
	n := &Node{tr: tr, disco: disco, log: logger.With("node", id), dataDir: cfg.DataDir, maxEntries: cfg.MaxEntries}
	n.mem = cluster.New(cluster.Member{ID: id, Addr: cfg.Addr}, tr, backups)
	n.maps = imap.NewService(n.mem, tr)
	// Seed with the initial table epoch so the first (self-only) table build is
	// not mistaken for a rebalance.
	n.lastMigratedVersion = n.mem.TableVersion()

	if n.dataDir != "" {
		if err := n.loadSnapshot(); err != nil {
			n.log.Warn("snapshot load failed", "err", err)
		}
		// Replay the write-ahead log on top of the snapshot and open it for
		// appending, so writes since the last snapshot survive an ungraceful
		// crash and subsequent writes are logged durably.
		if err := n.maps.OpenWAL(n.walPath()); err != nil {
			n.log.Warn("WAL open/replay failed", "err", err)
		}
		n.log.Info("persistence ready", "entries", n.maps.LocalEntryCount())
		n.lastSaveNano = time.Now().UnixNano()
	}

	if err := tr.Listen(n.dispatch); err != nil {
		// New won't return a Node, so Close (and thus CloseWAL) will never run —
		// release the WAL handle here to avoid leaking the open file.
		if n.dataDir != "" {
			_ = n.maps.CloseWAL()
		}
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
			if len(n.mem.Members()) <= 1 {
				// Isolated: (re)discover peers and try to join them. Discovery is
				// re-run every tick so a dynamic source (DNS) picks up pods that
				// appeared since startup; an empty result just means "no peers yet".
				jctx, cancel := context.WithTimeout(ctx, interval)
				switch seeds, err := n.disco.Discover(jctx); {
				case err != nil:
					n.log.Debug("peer discovery failed", "err", err)
				case len(seeds) == 0:
					// Standalone, or the discovery source has no peers yet — retry.
				default:
					if err := n.mem.Join(jctx, seeds); err != nil {
						// Expected while bootstrapping (peers not yet reachable); the
						// loop retries on the next tick.
						n.log.Debug("join attempt failed", "seeds", seeds, "err", err)
					}
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
				// Active anti-entropy: re-push a rotating slice of owned partitions
				// to their backups so a replica that missed a write converges. Bound
				// it to one interval so a slow/unreachable backup can't stall the
				// loop's gossip and failure detection.
				aeCtx, cancel := context.WithTimeout(ctx, interval)
				synced, cursor := n.maps.SyncBackups(aeCtx, n.mem.Table(), n.reconcileCursor, antiEntropyBatch)
				cancel()
				n.reconcileCursor = cursor
				if synced > 0 {
					metrics.Reconciled.Add(int64(synced))
				}
			}
			// When the partition table changes (a node joined, left, or was
			// evicted), move the data that this node no longer owns to its new
			// holders.
			if v := n.mem.TableVersion(); v != n.lastMigratedVersion {
				n.maps.Migrate(ctx, n.mem.Table())
				n.lastMigratedVersion = v
				metrics.Migrations.Add(1)
				n.log.Info("rebalanced partitions", "tableVersion", v, "members", len(n.mem.Members()))
				// A rebalance dropped partitions, so the snapshot+WAL must be
				// checkpointed before a crash could replay writes for partitions
				// this node no longer owns. Flag it for the unified checkpoint
				// below, which keeps retrying every tick until it succeeds.
				n.checkpointDue = true
			}
			// Reclaim expired entries (lazy expiry already hides them on read).
			if swept := n.maps.SweepExpired(); swept > 0 {
				metrics.Swept.Add(int64(swept))
			}
			// Enforce the soft per-node entry cap (evict owned entries, replicated
			// so backups stay consistent), bounded per tick so a big overshoot
			// drains over several ticks rather than stalling the loop.
			if n.maxEntries > 0 {
				if ev := n.maps.Evict(ctx, n.maxEntries, evictBatch); ev > 0 {
					metrics.EvictedEntries.Add(int64(ev))
					n.log.Info("evicted entries (max-size)", "count", ev, "max", n.maxEntries)
				}
			}
			// Checkpoint when one is due (post-rebalance or a prior failure) or the
			// periodic interval elapsed. On failure checkpointDue stays set, so the
			// next tick retries promptly rather than waiting a full interval.
			if n.dataDir != "" && (n.checkpointDue || time.Now().UnixNano()-n.lastSaveNano > int64(snapshotInterval)) {
				if err := n.saveSnapshot(); err != nil {
					n.log.Warn("snapshot save failed", "err", err)
				} else {
					n.lastSaveNano = time.Now().UnixNano()
					n.checkpointDue = false
				}
			}
		}
	}
}

func (n *Node) snapshotPath() string { return filepath.Join(n.dataDir, snapshotFile) }
func (n *Node) walPath() string      { return filepath.Join(n.dataDir, walFile) }

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

// saveSnapshot checkpoints this node's state: it writes a fresh snapshot to disk
// atomically (temp file + rename) and then truncates the write-ahead log, so the
// log only ever holds writes since the last snapshot. The two steps are
// serialized against concurrent writes by the Service so no logged write is
// dropped before the snapshot reflects it.
func (n *Node) saveSnapshot() error {
	return n.maps.Checkpoint(n.writeSnapshotFile)
}

// writeSnapshotFile persists snap to the data directory atomically and durably:
// it writes a temp file, fsyncs it, and only then renames it into place — so the
// snapshot's bytes are on stable storage before it replaces the previous one and
// before Checkpoint truncates the WAL. Without the fsync, a crash after the
// rename but before the OS flushed the data pages would leave a durably-truncated
// WAL alongside an unwritten snapshot: total loss of everything since the prior
// checkpoint. The temp file is removed on any failure so a botched write does not
// linger.
func (n *Node) writeSnapshotFile(snap *medusav1.Snapshot) error {
	if err := os.MkdirAll(n.dataDir, 0o755); err != nil {
		return err
	}
	data, err := snap.MarshalVT()
	if err != nil {
		return err
	}
	tmp := n.snapshotPath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, n.snapshotPath()); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
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
		medusav1.MessageType_MESSAGE_TYPE_EXECUTE_REQUEST,
		medusav1.MessageType_MESSAGE_TYPE_DIGEST_REQUEST,
		medusav1.MessageType_MESSAGE_TYPE_SIZE_REQUEST,
		medusav1.MessageType_MESSAGE_TYPE_CLEAR_REQUEST:
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

// BackupCount returns the number of backup copies kept per partition (the
// replication factor minus one), as currently realised by the partition table.
func (n *Node) BackupCount() int { return n.mem.Table().BackupCount() }

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
			if err := n.maps.CloseWAL(); err != nil {
				n.log.Warn("WAL close failed", "err", err)
			}
		}
	})
	return n.tr.Close()
}
