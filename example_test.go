package medusa_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"

	"github.com/lodgvideon/medusa"
)

// exampleAddr reserves a free loopback address for a single-node example. Addr
// must be concrete (not host:0) because it is the address advertised to peers.
func exampleAddr() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// Example shows the basic distributed map: store a value and read it back. A
// single node owns every partition, so the read is served locally; in a cluster
// the same calls route to the partition owner transparently.
func Example() {
	node, err := medusa.New(medusa.Config{ID: "a", Addr: exampleAddr()})
	if err != nil {
		panic(err)
	}
	defer node.Close()

	ctx := context.Background()
	users := node.Map("users")
	_ = users.Put(ctx, []byte("alice"), []byte("active"))

	v, ok, _ := users.Get(ctx, []byte("alice"))
	fmt.Printf("alice=%s found=%v\n", v, ok)
	// Output: alice=active found=true
}

// Example_atomicCounter uses the built-in "incr" EntryProcessor as an atomic
// distributed counter: each call is a read-modify-write executed on the key's
// owner under the shard lock, so concurrent increments never lose updates — one
// round trip, no data movement. The value and argument are big-endian int64.
func Example_atomicCounter() {
	node, _ := medusa.New(medusa.Config{ID: "a", Addr: exampleAddr()})
	defer node.Close()

	ctx := context.Background()
	counters := node.Map("counters")
	by := func(n int64) []byte {
		b := make([]byte, 8)
		binary.BigEndian.PutUint64(b, uint64(n))
		return b
	}

	_, _ = counters.Execute(ctx, []byte("hits"), "incr", by(2))
	out, _ := counters.Execute(ctx, []byte("hits"), "incr", by(3))
	fmt.Println(int64(binary.BigEndian.Uint64(out)))
	// Output: 5
}

// Example_fencedLock acquires a fenced lock, shows that a different holder is
// refused, then releases it. The returned fence token is monotonically
// increasing while the owner is live and lets a holder prove ownership to a
// downstream service.
func Example_fencedLock() {
	node, _ := medusa.New(medusa.Config{ID: "a", Addr: exampleAddr()})
	defer node.Close()

	ctx := context.Background()
	locks := node.Map("locks")

	token, acquired, _ := locks.Lock(ctx, []byte("job"), []byte("worker-1"))
	fmt.Printf("acquired=%v token=%d\n", acquired, token)

	_, contended, _ := locks.Lock(ctx, []byte("job"), []byte("worker-2"))
	fmt.Printf("contended=%v\n", contended)

	released, _ := locks.Unlock(ctx, []byte("job"), []byte("worker-1"))
	fmt.Printf("released=%v\n", released)
	// Output:
	// acquired=true token=1
	// contended=false
	// released=true
}
