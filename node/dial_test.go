package node

import (
	"context"
	"errors"
	"iter"
	"log/slog"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/iroh"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// answering is a lookup service that answers with addrs after delay.
func answering(delay time.Duration, addrs ...netaddr.TransportAddr) iroh.AddressResolver {
	return iroh.AddressResolverFunc(func(ctx context.Context, id irohkey.EndpointID) iter.Seq2[iroh.Item, error] {
		return func(yield func(iroh.Item, error) bool) {
			select {
			case <-time.After(delay):
				yield(iroh.NewItem(dns.EndpointInfo{ID: id, Data: dns.NewEndpointData(addrs...)}, "test", nil), nil)
			case <-ctx.Done():
				yield(iroh.Item{}, ctx.Err())
			}
		}
	})
}

// failing is a lookup service that fails at once.
func failing(msg string) iroh.AddressResolver {
	return iroh.AddressResolverFunc(func(context.Context, irohkey.EndpointID) iter.Seq2[iroh.Item, error] {
		return func(yield func(iroh.Item, error) bool) { yield(iroh.Item{}, errors.New(msg)) }
	})
}

// hanging is a lookup service that answers nothing until it is canceled.
func hanging() iroh.AddressResolver { return answering(time.Hour) }

func TestLookup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	id := newKey(t).Public().EndpointID()
	log := slog.New(slog.DiscardHandler)

	direct := netaddr.IPAddr{Addr: netip.MustParseAddrPort("192.168.1.9:4242")}
	other := netaddr.IPAddr{Addr: netip.MustParseAddrPort("203.0.113.7:4242")}
	relayURL, err := netaddr.ParseRelayURL("https://relay.example./")
	if err != nil {
		t.Fatal(err)
	}
	relay := netaddr.RelayAddr{URL: relayURL}

	contains := func(t *testing.T, got []netaddr.TransportAddr, want ...netaddr.TransportAddr) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for _, w := range want {
			if !slices.ContainsFunc(got, func(g netaddr.TransportAddr) bool { return g.Compare(w) == 0 }) {
				t.Fatalf("got %v, want %v", got, want)
			}
		}
	}

	t.Run("the answers are united", func(t *testing.T) {
		var services iroh.AddressLookupServices
		services.AddResolver(answering(0, direct))
		services.AddResolver(answering(50*time.Millisecond, relay, direct, other))
		services.AddResolver(failing("no such record"))
		got, err := lookup(ctx, &services, id, time.Minute, log)
		if err != nil {
			t.Fatal(err)
		}
		contains(t, got, direct, other, relay)
	})

	t.Run("a service that hangs is left out when the budget is spent", func(t *testing.T) {
		var services iroh.AddressLookupServices
		services.AddResolver(answering(0, direct))
		services.AddResolver(hanging())
		started := time.Now()
		got, err := lookup(ctx, &services, id, 200*time.Millisecond, log)
		if err != nil {
			t.Fatal(err)
		}
		contains(t, got, direct)
		if took := time.Since(started); took < 150*time.Millisecond || took > 5*time.Second {
			t.Fatalf("lookup took %v, want the budget of 200ms", took)
		}
	})

	t.Run("no answer is an error with the reason", func(t *testing.T) {
		var services iroh.AddressLookupServices
		services.AddResolver(failing("no such record"))
		_, err := lookup(ctx, &services, id, time.Minute, log)
		if err == nil || !strings.Contains(err.Error(), "no such record") {
			t.Fatalf("error %v, want one that carries the reason", err)
		}
	})

	t.Run("an answer without addresses is no answer", func(t *testing.T) {
		var services iroh.AddressLookupServices
		services.AddResolver(answering(0))
		if got, err := lookup(ctx, &services, id, time.Minute, log); err == nil {
			t.Fatalf("got %v and no error", got)
		}
	})
}
