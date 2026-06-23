// Package cluster tracks live membership and derives the partition table from
// it over the transport. Routing needs no coordinator: any two nodes that
// agree on the member set compute an identical partition assignment.
package cluster

import medusav1 "github.com/lodgvideon/medusa/genproto/medusa/v1"

// Member identifies a node in the cluster.
type Member struct {
	ID   string // stable, unique node id
	Addr string // transport address peers use to reach the node
}

func (m Member) toProto() *medusav1.Member {
	return &medusav1.Member{Id: m.ID, Addr: m.Addr}
}

func memberFromProto(p *medusav1.Member) Member {
	return Member{ID: p.GetId(), Addr: p.GetAddr()}
}
