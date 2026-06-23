package imap

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"sync"

	medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"
)

// wal is an append-only, fsync-on-write log of local store mutations. It closes
// the durability gap left by periodic snapshots: a record is appended and
// flushed to stable storage before its write is acknowledged, so an ungraceful
// crash (no graceful snapshot) loses no acknowledged write — replaying the WAL
// on top of the last snapshot reconstructs the store.
//
// Records are framed as [uint32 big-endian payload length][1 byte op][payload],
// where the payload is a marshalled SnapshotEntry (reused so no new schema is
// needed). A torn trailing record — written but not fully flushed before a crash
// — is detected and dropped on replay; such a write was never acknowledged.
//
// The mutex also serialises checkpoints (see Service.Checkpoint): truncation
// happens under it, so it cannot race a concurrent append. (Group-committing the
// fsync was explored but coalesced poorly here and added concurrency complexity
// for little gain; it stays on the roadmap for benchmarking on the deploy OS.)
type wal struct {
	mu sync.Mutex
	f  *os.File
}

const (
	walOpPut    byte = 1
	walOpRemove byte = 2
)

// openWAL opens (creating if needed) the log at path and positions the write
// offset at the end so new records append after any existing ones. We do not
// use O_APPEND: all writes are serialized under w.mu (so atomic-append is
// unnecessary), and a plain offset lets truncateLocked reliably rewind to the
// start on every platform — O_APPEND ignores seeks, which on Windows leaves a
// zero gap after a truncate.
func openWAL(path string) (*wal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &wal{f: f}, nil
}

func (w *wal) appendPut(name string, key, value []byte, expireAt int64) error {
	return w.append(walOpPut, &medusav1.SnapshotEntry{Map: name, Key: key, Value: value, ExpireAt: expireAt})
}

func (w *wal) appendRemove(name string, key []byte) error {
	return w.append(walOpRemove, &medusav1.SnapshotEntry{Map: name, Key: key})
}

// append frames and writes one record, then fsyncs so the write is durable
// before the caller acknowledges it.
func (w *wal) append(op byte, e *medusav1.SnapshotEntry) error {
	payload, err := e.MarshalVT()
	if err != nil {
		return err
	}
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[:4], uint32(len(payload)))
	hdr[4] = op

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return os.ErrClosed
	}
	if _, err := w.f.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.f.Write(payload); err != nil {
		return err
	}
	return w.f.Sync()
}

// truncateLocked empties the log after a checkpoint has durably captured every
// record's effect in a snapshot. The caller must hold w.mu (Service.Checkpoint
// holds it across snapshot capture + truncate so no record is dropped before
// the snapshot reflects it).
func (w *wal) truncateLocked() error {
	if w.f == nil {
		return os.ErrClosed
	}
	if err := w.f.Truncate(0); err != nil {
		return err
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *wal) close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// replayWAL reads every intact record from the log at path in order, invoking
// put or del for each. It returns the byte length of the valid prefix — the
// offset just past the last fully-applied record — so the caller can chop a
// torn or corrupt tail (the remnant of an interrupted write) and keep the log
// contiguous; otherwise records appended after that remnant would be unreadable
// on a later replay. A missing file is not an error (first start). Reading stops
// cleanly at the first torn or corrupt record, since such a record was never
// acknowledged.
func replayWAL(path string, put func(name string, key, value []byte, expireAt int64), del func(name string, key []byte)) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	var valid int64
	var hdr [5]byte
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return valid, nil // clean EOF or torn header: end of the usable log
		}
		n := binary.BigEndian.Uint32(hdr[:4])
		op := hdr[4]
		payload := make([]byte, n)
		if _, err := io.ReadFull(r, payload); err != nil {
			return valid, nil // torn final record: stop at the last good one
		}
		var e medusav1.SnapshotEntry
		if err := e.UnmarshalVT(payload); err != nil {
			return valid, nil // corrupt record: stop
		}
		switch op {
		case walOpPut:
			put(e.Map, e.Key, e.Value, e.ExpireAt)
		case walOpRemove:
			del(e.Map, e.Key)
		}
		valid += int64(len(hdr)) + int64(n)
	}
}

// truncateTail trims the file at path to validLen bytes when it is longer,
// dropping a torn or corrupt tail left by an interrupted write. A missing file
// is a no-op.
func truncateTail(path string, validLen int64) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if info.Size() <= validLen {
		return nil
	}
	return os.Truncate(path, validLen)
}
