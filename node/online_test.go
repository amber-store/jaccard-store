package node

import (
	"context"
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/netaddr"
)

// TestOnline binds a server the way a deployed one is bound, with the
// default relays, pkarr and mDNS, and reaches it with nothing but its
// endpoint ID: once as Dial does, and once through the relay alone, which
// is the path of a client that reaches no direct address. It needs the
// internet and number0's relays and DNS server, so it runs only when
// JACCARD_TEST_ONLINE=1.
func TestOnline(t *testing.T) {
	if os.Getenv("JACCARD_TEST_ONLINE") != "1" {
		t.Skip("set JACCARD_TEST_ONLINE=1 to run the test that needs the internet")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// The goroutines of Bind and Dial may log after the test has ended,
	// which the log of a test does not allow.
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	serverKey := newKey(t)
	server := serverKey.Public().EndpointID()
	announcing, stop := context.WithCancel(ctx)
	ep, err := Bind(announcing, ServerConfig{Key: serverKey, ALPN: testALPN, Log: log})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ep.Accept(announcing)
			if err != nil {
				return
			}
			go echoStreams(announcing, conn)
		}
	}()
	defer func() {
		stop()
		<-done
		ep.Shutdown(context.Background())
	}()

	// The first record names the first of the default relays. The endpoint
	// then measures which relay is nearest and moves its home there, and
	// the record has to follow. This comes first so that nothing asks DNS
	// for the record before it is there: a resolver may remember that it
	// was not.
	t.Run("the pkarr record names the home relay", func(t *testing.T) {
		pkarr, err := iroh.N0PkarrResolver(nil)
		if err != nil {
			t.Fatal(err)
		}
		for measured := false; !measured && ctx.Err() == nil; time.Sleep(200 * time.Millisecond) {
			_, measured = ep.NetReport()
		}
		var last string
		for ctx.Err() == nil {
			want := ep.Addr().RelayURLs()
			for item, err := range pkarr.Resolve(ctx, server) {
				if err != nil {
					last = err.Error()
					continue
				}
				got := item.Addr()
				last = got.String()
				if len(want) > 0 && len(got.IPAddrs()) > 0 &&
					slices.EqualFunc(got.RelayURLs(), want, netaddr.RelayURL.Equal) {
					return
				}
			}
			time.Sleep(2 * time.Second)
		}
		t.Fatalf("the record never named the home relay %v; last answer: %s", ep.Addr().RelayURLs(), last)
	})

	t.Run("by ID alone", func(t *testing.T) {
		dialCtx, stopDial := context.WithTimeout(ctx, 30*time.Second)
		defer stopDial()
		conn, err := Dial(dialCtx, DialConfig{Key: newKey(t), ALPN: testALPN, Server: server, Log: log})
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		if got := conn.RemoteID(); got != server {
			t.Fatalf("connected to %v, want %v", got, server)
		}
		got, err := exchange(ctx, conn, "online.1")
		if err != nil {
			t.Fatal(err)
		}
		if got != "re:online.1" {
			t.Fatalf("answer %q", got)
		}
	})

	t.Run("through the relay alone", func(t *testing.T) {
		cfg := DialConfig{Key: newKey(t), ALPN: testALPN, Server: server, Log: log}
		lookups, stopLookups := context.WithCancel(context.Background())
		defer stopLookups()
		client, cands, err := bindAndResolve(ctx, lookups, cfg, log)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Shutdown(context.Background())

		var relays []netaddr.TransportAddr
		for _, cand := range cands {
			if _, ok := cand.(netaddr.RelayAddr); ok {
				relays = append(relays, cand)
			}
		}
		if len(relays) == 0 {
			t.Fatalf("no relay among the candidates %v", cands)
		}
		dialCtx, stopDial := context.WithTimeout(ctx, 30*time.Second)
		defer stopDial()
		raw, err := raceConnect(dialCtx, client, server, testALPN, relays)
		if err != nil {
			t.Fatalf("connect through %v: %v", relays, err)
		}
		conn := &Conn{conn: raw, ep: client, stop: stopLookups}
		defer conn.Close()

		got, err := exchange(ctx, conn, "relayed1")
		if err != nil {
			t.Fatal(err)
		}
		if got != "re:relayed1" {
			t.Fatalf("answer %q", got)
		}
	})
}
