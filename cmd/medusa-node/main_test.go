package main

import (
	"testing"

	"github.com/lodgvideon/medusa/discovery"
)

func TestDiscovererFromEnv(t *testing.T) {
	t.Run("unset falls back to static seeds", func(t *testing.T) {
		d, desc := discovererFromEnv("", ":7700", []string{"x:7700"})
		s, ok := d.(discovery.Static)
		if !ok || len(s) != 1 || s[0] != "x:7700" {
			t.Fatalf("got %T %v", d, d)
		}
		if desc != "static" {
			t.Fatalf("desc = %q", desc)
		}
	})

	t.Run("explicit static", func(t *testing.T) {
		if _, ok := discovererFromEnvDisco(t, "static", ":7700", nil).(discovery.Static); !ok {
			t.Fatal("want Static")
		}
	})

	t.Run("dns host inherits the data-plane port", func(t *testing.T) {
		d, desc := discovererFromEnv("dns:medusa", "medusa-0.medusa:7700", nil)
		dns, ok := d.(*discovery.DNS)
		if !ok {
			t.Fatalf("got %T", d)
		}
		if dns.Host != "medusa" || dns.Port != "7700" {
			t.Fatalf("host=%q port=%q", dns.Host, dns.Port)
		}
		if desc != "dns:medusa:7700" {
			t.Fatalf("desc = %q", desc)
		}
	})

	t.Run("dns host with explicit port", func(t *testing.T) {
		d, _ := discovererFromEnv("dns:medusa:9000", ":7700", nil)
		dns := d.(*discovery.DNS)
		if dns.Host != "medusa" || dns.Port != "9000" {
			t.Fatalf("host=%q port=%q", dns.Host, dns.Port)
		}
	})

	t.Run("dns host with trailing colon keeps the derived port", func(t *testing.T) {
		// "dns:medusa:" (empty port segment) must not clobber the derived port
		// with "", which would build invalid "ip:" dial targets.
		d, desc := discovererFromEnv("dns:medusa:", "medusa-0.medusa:7700", nil)
		dns := d.(*discovery.DNS)
		if dns.Host != "medusa" || dns.Port != "7700" {
			t.Fatalf("host=%q port=%q", dns.Host, dns.Port)
		}
		if desc != "dns:medusa:7700" {
			t.Fatalf("desc = %q", desc)
		}
	})

	t.Run("malformed dns spec falls back to static", func(t *testing.T) {
		if _, ok := discovererFromEnvDisco(t, "dns:", ":7700", nil).(discovery.Static); !ok {
			t.Fatal("empty dns host should fall back to static")
		}
	})
}

func discovererFromEnvDisco(t *testing.T, spec, addr string, seeds []string) discovery.Discoverer {
	t.Helper()
	d, _ := discovererFromEnv(spec, addr, seeds)
	return d
}

func TestPortOf(t *testing.T) {
	cases := []struct{ addr, def, want string }{
		{":7700", "7700", "7700"},
		{"medusa-0.medusa:7700", "9999", "7700"},
		{"no-port", "7700", "7700"},
		{"", "7700", "7700"},
	}
	for _, c := range cases {
		if got := portOf(c.addr, c.def); got != c.want {
			t.Fatalf("portOf(%q, %q) = %q, want %q", c.addr, c.def, got, c.want)
		}
	}
}
