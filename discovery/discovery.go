// Package discovery resolves the set of peer addresses a node contacts to join
// and stay in a cluster. It decouples *how peers are found* from membership: a
// Static list reproduces the classic seed-list behaviour, while DNS resolves a
// name (e.g. a Kubernetes headless Service) to the current set of pods — so a
// cluster self-assembles and scales out without a hand-maintained seed list.
//
// The addresses a Discoverer returns are only *dial targets* used to bootstrap
// contact; the real advertised member addresses are learned from the join and
// gossip exchange. A Discoverer may therefore safely return raw IPs (e.g. the
// pod IPs behind a headless Service) and may include this node's own address —
// membership ignores self by id.
package discovery

import (
	"context"
	"net"
	"sort"
)

// Discoverer returns peer addresses ("host:port") to contact for cluster
// bootstrap. An empty result is not an error — it means "no peers known yet",
// and the caller is expected to retry on its next maintenance tick.
type Discoverer interface {
	Discover(ctx context.Context) ([]string, error)
}

// Static is a fixed list of seed addresses — the classic, dependency-free
// discovery mode. A nil or empty Static yields no peers (a standalone node).
type Static []string

// Discover returns the static list unchanged.
func (s Static) Discover(context.Context) ([]string, error) {
	return []string(s), nil
}

// LookupHostFunc resolves a hostname to a set of IP-address strings. It matches
// the signature of net.Resolver.LookupHost and is swappable in tests.
type LookupHostFunc func(ctx context.Context, host string) ([]string, error)

// DNS discovers peers by resolving Host to its A/AAAA records and pairing each
// resolved address with Port. Pointed at a Kubernetes headless Service it
// returns every pod backing that Service, so scaling the StatefulSet up or down
// needs no seed-list change — the next maintenance tick simply sees the new set.
type DNS struct {
	Host string
	Port string
	// Lookup resolves Host; defaults to the system resolver. Overridden in tests.
	Lookup LookupHostFunc
}

// NewDNS returns a DNS discoverer for host:port using the system resolver.
func NewDNS(host, port string) *DNS {
	return &DNS{Host: host, Port: port, Lookup: net.DefaultResolver.LookupHost}
}

// Discover resolves Host and returns one "ip:Port" entry per resolved address,
// sorted for determinism. A resolution error is propagated so the caller can
// retry — transient DNS failures are expected during a rollout, when the
// Service's endpoint set is still settling.
func (d *DNS) Discover(ctx context.Context) ([]string, error) {
	ips, err := d.Lookup(ctx, d.Host)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ips))
	for _, ip := range ips {
		out = append(out, net.JoinHostPort(ip, d.Port))
	}
	sort.Strings(out)
	return out, nil
}
