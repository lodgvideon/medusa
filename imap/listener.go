package imap

import (
	"sync"
	"sync/atomic"

	"github.com/lodgvideon/medusa/metrics"
)

// EventType classifies an entry mutation delivered to a listener.
type EventType uint8

const (
	EventCreated EventType = iota // a key that did not exist now holds a value
	EventUpdated                  // an existing key's value changed
	EventRemoved                  // a key was deleted (explicit remove or max-size eviction)
)

func (t EventType) String() string {
	switch t {
	case EventCreated:
		return "created"
	case EventUpdated:
		return "updated"
	case EventRemoved:
		return "removed"
	default:
		return "unknown"
	}
}

// EntryEvent describes one mutation, observed on the partition OWNER. Key and
// Value are private copies the listener may retain; Value is nil for EventRemoved.
type EntryEvent struct {
	Type  EventType
	Map   string
	Key   []byte
	Value []byte
}

// EntryListener is an injected handler invoked for entry events — the seam for
// integrating medusa with external systems (an audit log, a message bus, a cache
// invalidator) without the core knowing about them (dependency inversion: the
// core depends only on this abstraction). Listeners run on a background
// dispatcher, never on the write path, so a slow integration cannot stall or
// deadlock a mutation; but a listener that blocks forever stalls delivery to the
// others, and a listener MUST NOT call back into the Map (that would re-enter the
// same mutation paths). Events are local: a listener fires for mutations the node
// it is registered on owns, so for a cluster-wide view register on every node.
type EntryListener func(EntryEvent)

// listenerQueue bounds the backlog of undelivered events. A slow listener drops
// events past this (counted in medusa_events_dropped_total) rather than blocking
// writes — delivery is best-effort, consistent with the system's AP stance.
const listenerQueue = 1024

// listeners is the registry and asynchronous dispatcher. Delivery is off the
// write path, and gated by a single atomic flag so the hot path pays nothing —
// no allocation, no channel send — until at least one listener is registered.
type listeners struct {
	active atomic.Bool  // fast gate read on every mutation; set once a listener exists
	fns    atomic.Value // holds []EntryListener, replaced (never mutated) on add
	ch     chan EntryEvent
	done   chan struct{}

	mu     sync.Mutex // guards add (append + dispatcher start) against close
	closed bool
}

func newListeners() *listeners {
	return &listeners{ch: make(chan EntryEvent, listenerQueue), done: make(chan struct{})}
}

// add registers a listener, starting the dispatcher goroutine on the first one.
func (l *listeners) add(fn EntryListener) {
	if fn == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	prev, _ := l.fns.Load().([]EntryListener)
	next := make([]EntryListener, len(prev)+1) // copy-on-write so the dispatcher reads a stable slice
	copy(next, prev)
	next[len(prev)] = fn
	// Arm the gate BEFORE publishing the listener slice. A concurrent emit() that
	// reads active==true in the window before fns.Store enqueues its event (the
	// channel is buffered); the dispatcher — started after fns.Store — then loads
	// the populated slice and delivers it. If the order were reversed, an emit in
	// the gap would read active==false and silently drop the event, even though a
	// listener is already registered.
	l.active.Store(true)
	l.fns.Store(next)
	if len(prev) == 0 {
		go l.dispatch()
	}
}

// emit enqueues an event for asynchronous delivery. It is a no-op — and crucially
// allocation-free — when no listener is registered, so the write hot path is
// unaffected until the feature is used. A full queue drops the event (counted)
// rather than blocking the writer.
func (l *listeners) emit(t EventType, name string, key, value []byte) {
	if !l.active.Load() {
		return
	}
	ev := EntryEvent{Type: t, Map: name, Key: cloneBytes(key), Value: cloneBytes(value)}
	select {
	case l.ch <- ev:
		metrics.EventsEmitted.Add(1)
	default:
		metrics.EventsDropped.Add(1)
	}
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// dispatch delivers queued events to every listener, one event at a time, until
// close. It reads the listener slice with an atomic load (no lock, no per-event
// allocation), so registering a listener never contends with delivery.
func (l *listeners) dispatch() {
	for {
		select {
		case <-l.done:
			return
		case ev := <-l.ch:
			fns, _ := l.fns.Load().([]EntryListener)
			for _, fn := range fns {
				fn(ev)
			}
		}
	}
}

// close stops the dispatcher. Safe to call once; further emits are dropped.
func (l *listeners) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.closed = true
	l.active.Store(false)
	close(l.done)
}
