package discovery_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/lodgvideon/medusa/discovery"
)

func TestStaticReturnsSeedsUnchanged(t *testing.T) {
	seeds := discovery.Static{"a:7700", "b:7700"}
	got, err := seeds.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"a:7700", "b:7700"}) {
		t.Fatalf("got %v", got)
	}
}

func TestStaticNilYieldsNoPeers(t *testing.T) {
	got, err := discovery.Static(nil).Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no peers, got %v", got)
	}
}

func TestDNSResolvesAndAppendsPortSorted(t *testing.T) {
	d := discovery.NewDNS("medusa", "7700")
	called := ""
	d.Lookup = func(_ context.Context, host string) ([]string, error) {
		called = host
		// Deliberately unsorted to prove Discover sorts for determinism.
		return []string{"10.0.0.3", "10.0.0.1", "10.0.0.2"}, nil
	}
	got, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if called != "medusa" {
		t.Fatalf("looked up %q, want medusa", called)
	}
	want := []string{"10.0.0.1:7700", "10.0.0.2:7700", "10.0.0.3:7700"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestDNSResolutionErrorPropagates(t *testing.T) {
	d := discovery.NewDNS("nope", "7700")
	sentinel := errors.New("nxdomain")
	d.Lookup = func(context.Context, string) ([]string, error) { return nil, sentinel }
	if _, err := d.Discover(context.Background()); !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want %v", err, sentinel)
	}
}

func TestDNSEmptyResultIsNotAnError(t *testing.T) {
	d := discovery.NewDNS("medusa", "7700")
	d.Lookup = func(context.Context, string) ([]string, error) { return nil, nil }
	got, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected empty, got %v", got)
	}
}

func TestDNSIPv6IsBracketed(t *testing.T) {
	d := discovery.NewDNS("medusa", "7700")
	d.Lookup = func(context.Context, string) ([]string, error) {
		return []string{"fd00::1"}, nil
	}
	got, err := d.Discover(context.Background())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	if !reflect.DeepEqual(got, []string{"[fd00::1]:7700"}) {
		t.Fatalf("got %v, want bracketed IPv6 host:port", got)
	}
}

// NewDNS must wire a real resolver so the zero-config constructor works without
// a manual Lookup override.
func TestNewDNSHasDefaultResolver(t *testing.T) {
	if discovery.NewDNS("h", "1").Lookup == nil {
		t.Fatal("NewDNS left Lookup nil")
	}
}
