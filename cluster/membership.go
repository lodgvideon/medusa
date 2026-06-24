package cluster

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/lodgvideon/medusa/codec"
	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
	"github.com/lodgvideon/medusa/partition"
	"github.com/lodgvideon/medusa/transport"
)

// Membership tracks the set of live members and derives the partition table
// from it. Mutations bump a version counter and rebuild the table under the
// write lock; reads take the read lock, so routing lookups stay cheap and
// contention-free on the hot path.
type Membership struct {
	self    Member
	tr      transport.Transport
	backups int // backups per partition; threaded into every partition table built

	mu       sync.RWMutex
	members  map[string]Member
	version  uint64 // bumps on any membership change (incl. address-only updates)
	tableVer uint64 // bumps only when the partition table is rebuilt (the id set changed)
	table    *partition.Table
	misses   map[string]uint8       // consecutive failed heartbeats per member id
	phi      map[string]*phiHistory // per-peer heartbeat-interval history for phi-accrual detection
	removed  map[string]int64       // tombstones (id -> unix nano removed) suppressing gossip re-add
}

// New creates a Membership containing only self and bound to tr. backups is the
// number of backup copies each partition keeps (the replication factor minus
// one); it is baked into every partition table this membership derives. The
// caller must register a transport handler that dispatches cluster message
// types to (*Membership).Handle (the top-level node does this).
func New(self Member, tr transport.Transport, backups int) *Membership {
	m := &Membership{
		self:    self,
		tr:      tr,
		backups: backups,
		members: map[string]Member{self.ID: self},
		misses:  map[string]uint8{},
		phi:     map[string]*phiHistory{},
		removed: map[string]int64{},
	}
	m.rebuildLocked()
	return m
}

// rebuildLocked recomputes the partition table from the current member set and
// bumps tableVer. The caller must hold mu for writing. tableVer changes only
// here — i.e. only when the id set changed — so callers can distinguish a real
// topology change (which warrants data migration) from an address-only update.
func (m *Membership) rebuildLocked() {
	ids := make([]string, 0, len(m.members))
	for id := range m.members {
		ids = append(ids, id)
	}
	m.table = partition.NewTable(ids, m.backups)
	m.tableVer++
}

// TableVersion returns the partition-table epoch — incremented only when the
// table is rebuilt (a member joined or left), not for address-only changes.
func (m *Membership) TableVersion() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tableVer
}

// Self returns this node's own member identity.
func (m *Membership) Self() Member { return m.self }

// Table returns the current partition table. The returned table is immutable;
// a membership change publishes a fresh one.
func (m *Membership) Table() *partition.Table {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.table
}

// Version returns the current membership epoch.
func (m *Membership) Version() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.version
}

// Members returns the current members sorted by id.
func (m *Membership) Members() []Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Member, 0, len(m.members))
	for _, mem := range m.members {
		out = append(out, mem)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// AddrOf returns the transport address of member id.
func (m *Membership) AddrOf(id string) (string, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	mem, ok := m.members[id]
	return mem.Addr, ok
}

// merge folds the given members into the view, returning true if anything
// changed. New ids are added; a known id whose address changed (e.g. a node
// that restarted at a new address) is updated so peers heal automatically.
// Membership only grows here; shrinking happens via Remove.
func (m *Membership) merge(members []Member) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	changed, setChanged := false, false
	for _, mem := range members {
		if mem.ID == "" || mem.ID == m.self.ID {
			continue
		}
		if _, tomb := m.removed[mem.ID]; tomb {
			continue // a peer still gossiping a node we evicted — ignore it
		}
		switch existing, ok := m.members[mem.ID]; {
		case !ok:
			m.members[mem.ID] = mem
			changed, setChanged = true, true
		case mem.Addr != "" && existing.Addr != mem.Addr:
			m.members[mem.ID] = mem // address changed — adopt the new one
			changed = true
		}
	}
	if changed {
		m.version++
		if setChanged {
			m.rebuildLocked() // partition table depends only on the id set
		}
	}
	return changed
}

// Remove drops member id from the view, returning true if it was present. Self
// is never removed.
func (m *Membership) Remove(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.removeLocked(id)
}

// removeLocked is Remove's body; the caller must hold mu for writing. It exists
// so DetectFailures can increment the miss counter, test the threshold, and
// evict as one atomic critical section — otherwise a concurrent rejoin could
// slip between the decision and the removal and the node would evict a peer that
// had just explicitly rejoined.
func (m *Membership) removeLocked(id string) bool {
	if id == m.self.ID {
		return false
	}
	if _, ok := m.members[id]; !ok {
		return false
	}
	delete(m.members, id)
	delete(m.misses, id)
	delete(m.phi, id)                     // a rejoining peer rebuilds its heartbeat history from scratch
	m.removed[id] = time.Now().UnixNano() // tombstone so gossip does not resurrect it
	m.version++
	m.rebuildLocked()
	return true
}

// rejoin force-adds a member, clearing any tombstone. It is used for an
// explicit JOIN — a strong "I am here" signal that overrides a prior eviction,
// so a crashed node that comes back (same id) is readmitted.
func (m *Membership) rejoin(mem Member) {
	if mem.ID == "" || mem.ID == m.self.ID {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.removed, mem.ID)
	delete(m.misses, mem.ID) // an explicit JOIN proves liveness — reset the failure count
	delete(m.phi, mem.ID)    // and the heartbeat history, so the first ping after a
	// restart does not record a downtime-spanning interval that would corrupt the
	// estimator (matches removeLocked; a rejoining peer starts its history fresh).
	if existing, ok := m.members[mem.ID]; !ok || existing.Addr != mem.Addr {
		m.members[mem.ID] = mem
		m.version++
		m.rebuildLocked()
	}
}

func (m *Membership) snapshotProto() *medusav1.MemberList {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ml := &medusav1.MemberList{
		Version: m.version,
		Members: make([]*medusav1.Member, 0, len(m.members)),
	}
	for _, mem := range m.members {
		ml.Members = append(ml.Members, mem.toProto())
	}
	return ml
}

// Join contacts each seed, merges its membership view, then announces the
// merged view to all known peers so they learn about this node. It returns an
// error only if no seed could be reached.
func (m *Membership) Join(ctx context.Context, seeds []string) error {
	var lastErr error
	reached := false
	dst := make([]byte, 0, 512)

	for _, seed := range seeds {
		if seed == m.self.Addr {
			continue
		}
		members, ndst, err := m.sendJoin(ctx, seed, dst)
		dst = ndst
		if err != nil {
			lastErr = err
			continue
		}
		reached = true
		m.merge(members)
	}

	if !reached {
		return lastErr // nil when seeds was empty or only contained self
	}
	m.Gossip(ctx)
	return nil
}

func (m *Membership) sendJoin(ctx context.Context, seed string, dst []byte) ([]Member, []byte, error) {
	req := medusav1.JoinRequest{Candidate: m.self.toProto()}
	reqBytes, err := codec.Marshal(nil, &req)
	if err != nil {
		return nil, dst, err
	}
	respType, resp, err := m.tr.Send(ctx, seed,
		medusav1.MessageType_MESSAGE_TYPE_JOIN_REQUEST, reqBytes, dst)
	if err != nil {
		return nil, resp, err
	}
	if respType != medusav1.MessageType_MESSAGE_TYPE_JOIN_RESPONSE {
		return nil, resp, fmt.Errorf("cluster: unexpected join response type %v", respType)
	}
	var jr medusav1.JoinResponse
	if err := jr.UnmarshalVT(resp); err != nil {
		return nil, resp, err
	}
	out := make([]Member, 0, len(jr.Members))
	for _, p := range jr.Members {
		out = append(out, memberFromProto(p))
	}
	return out, resp, nil
}

// Gossip pushes the current membership view to every other member. It is the
// anti-entropy step that converges the cluster after a join or leave.
func (m *Membership) Gossip(ctx context.Context) {
	payload, err := codec.Marshal(nil, m.snapshotProto())
	if err != nil {
		return
	}
	dst := make([]byte, 0, 512)
	for _, peer := range m.Members() {
		if peer.ID == m.self.ID {
			continue
		}
		_, ndst, _ := m.tr.Send(ctx, peer.Addr,
			medusav1.MessageType_MESSAGE_TYPE_MEMBER_LIST, payload, dst)
		dst = ndst
	}
}

// Ping sends a heartbeat to addr, returning an error if the peer is unreachable.
func (m *Membership) Ping(ctx context.Context, addr string) error {
	hb := medusav1.Heartbeat{MemberId: m.self.ID}
	payload, err := codec.Marshal(nil, &hb)
	if err != nil {
		return err
	}
	_, _, err = m.tr.Send(ctx, addr,
		medusav1.MessageType_MESSAGE_TYPE_HEARTBEAT, payload, nil)
	return err
}

// CheckLiveness pings every peer and removes any that fail to respond,
// returning the evicted ids. This detector is intentionally blunt — a single
// missed heartbeat evicts — and is kept for tests and one-shot liveness checks.
// The maintenance loop instead uses DetectFailures, whose phi-accrual detector
// tolerates transient blips (see phi.go).
func (m *Membership) CheckLiveness(ctx context.Context) []string {
	var evicted []string
	for _, peer := range m.Members() {
		if peer.ID == m.self.ID {
			continue
		}
		if err := m.Ping(ctx, peer.Addr); err != nil {
			if m.Remove(peer.ID) {
				evicted = append(evicted, peer.ID)
			}
		}
	}
	return evicted
}

// DetectFailures pings every peer once and evicts those judged dead, returning
// the ids evicted (evicting bumps the membership version, which triggers a
// rebalance and data migration on the surviving nodes). Once a peer has enough
// heartbeat history it is judged by phi-accrual (see phi.go): suspicion rises
// smoothly with the silence since its last reply, scaled by the link's observed
// jitter, and it is evicted when phi reaches defaultPhiThreshold. Until then the
// simpler rule applies — evict after `threshold` consecutive missed heartbeats —
// so a transient blip never removes a healthy node. A successful heartbeat
// records the interval (feeding the estimator) and resets the miss count.
func (m *Membership) DetectFailures(ctx context.Context, threshold uint8) []string {
	if threshold == 0 {
		threshold = 1
	}
	var evicted []string
	for _, peer := range m.Members() {
		if peer.ID == m.self.ID {
			continue
		}
		err := m.Ping(ctx, peer.Addr)
		now := time.Now().UnixNano()
		// Record/decide as one critical section so a concurrent rejoin cannot slip
		// between the decision and the removal. Only touch a still-present member:
		// a concurrent path (e.g. a LEAVE) may have evicted it after the Members
		// snapshot, and touching it then would orphan an entry removeLocked (which
		// returns false for an absent member) never cleans up.
		m.mu.Lock()
		if _, ok := m.members[peer.ID]; ok {
			h := m.phi[peer.ID]
			if h == nil {
				h = newPhiHistory()
				m.phi[peer.ID] = h
			}
			if err == nil {
				h.record(now)             // feed the estimator
				delete(m.misses, peer.ID) // healthy: reset the miss count
			} else {
				m.misses[peer.ID]++
				// A warm peer is judged by phi-accrual (jitter-adaptive); until then
				// fall back to the fixed consecutive-miss count.
				var suspect bool
				if h.warm() {
					suspect = h.phi(now) >= defaultPhiThreshold
				} else {
					suspect = m.misses[peer.ID] >= threshold
				}
				if suspect && m.removeLocked(peer.ID) {
					evicted = append(evicted, peer.ID)
				}
			}
		}
		m.mu.Unlock()
	}
	return evicted
}

// Handle processes inbound cluster control messages. It satisfies the
// transport.Handler shape so the node's dispatcher can route JOIN_REQUEST,
// MEMBER_LIST and HEARTBEAT here.
func (m *Membership) Handle(reqType medusav1.MessageType, req, respBuf []byte) (medusav1.MessageType, []byte, error) {
	switch reqType {
	case medusav1.MessageType_MESSAGE_TYPE_JOIN_REQUEST:
		var jr medusav1.JoinRequest
		if err := jr.UnmarshalVT(req); err != nil {
			return 0, respBuf, err
		}
		if jr.Candidate != nil {
			m.rejoin(memberFromProto(jr.Candidate))
		}
		resp := medusav1.JoinResponse{Members: m.snapshotProto().Members}
		out, err := codec.Marshal(respBuf, &resp)
		return medusav1.MessageType_MESSAGE_TYPE_JOIN_RESPONSE, out, err

	case medusav1.MessageType_MESSAGE_TYPE_MEMBER_LIST:
		var ml medusav1.MemberList
		if err := ml.UnmarshalVT(req); err != nil {
			return 0, respBuf, err
		}
		members := make([]Member, 0, len(ml.Members))
		for _, p := range ml.Members {
			members = append(members, memberFromProto(p))
		}
		m.merge(members)
		return medusav1.MessageType_MESSAGE_TYPE_MEMBER_LIST, respBuf[:0], nil

	case medusav1.MessageType_MESSAGE_TYPE_HEARTBEAT:
		return medusav1.MessageType_MESSAGE_TYPE_HEARTBEAT, respBuf[:0], nil

	case medusav1.MessageType_MESSAGE_TYPE_LEAVE:
		var lr medusav1.LeaveRequest
		if err := lr.UnmarshalVT(req); err != nil {
			return 0, respBuf, err
		}
		m.Remove(lr.MemberId)
		return medusav1.MessageType_MESSAGE_TYPE_LEAVE, respBuf[:0], nil

	default:
		return 0, respBuf, fmt.Errorf("cluster: unhandled message type %v", reqType)
	}
}

// AnnounceLeave tells every peer that this node is departing, so they remove it
// and rebalance immediately instead of waiting for failure detection.
func (m *Membership) AnnounceLeave(ctx context.Context) {
	req := medusav1.LeaveRequest{MemberId: m.self.ID}
	payload, err := codec.Marshal(nil, &req)
	if err != nil {
		return
	}
	dst := make([]byte, 0, 64)
	for _, peer := range m.Members() {
		if peer.ID == m.self.ID {
			continue
		}
		_, ndst, _ := m.tr.Send(ctx, peer.Addr,
			medusav1.MessageType_MESSAGE_TYPE_LEAVE, payload, dst)
		dst = ndst
	}
}

// PruneTombstones drops tombstones older than ttl, bounding the set in a
// long-running cluster. After a tombstone expires the node may be re-learned
// via gossip — safe because a genuinely dead node is no longer gossiped by
// anyone once every peer has evicted it, and a node that legitimately returns
// readmits itself with an explicit JOIN.
func (m *Membership) PruneTombstones(ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ttl <= 0 {
		clear(m.removed) // flush everything
		return
	}
	cutoff := time.Now().UnixNano() - int64(ttl)
	for id, at := range m.removed {
		if at < cutoff {
			delete(m.removed, id)
		}
	}
}

// TableWithout returns the partition table this cluster would have if member id
// were gone. A leaving node uses it to hand its data to the right successors
// before it departs.
func (m *Membership) TableWithout(id string) *partition.Table {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]string, 0, len(m.members))
	for mid := range m.members {
		if mid != id {
			ids = append(ids, mid)
		}
	}
	return partition.NewTable(ids, m.backups)
}
