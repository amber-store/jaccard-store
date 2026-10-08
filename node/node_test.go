package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/tmc/go-iroh/iroh"
	irohkey "github.com/tmc/go-iroh/key"
)

const (
	testALPN    = "jaccard-store/node-test/1"
	testTimeout = 30 * time.Second
	messageSize = 8
)

// localServer is a server endpoint bound with Local. It answers every
// stream of every connection: it reads one message and writes it back with
// "re:" in front. The endpoint ID of every peer that connected is sent to
// peers.
type localServer struct {
	ep    *iroh.Endpoint
	key   irohkey.SecretKey
	peers chan irohkey.EndpointID
}

func startLocalServer(t *testing.T, ctx context.Context) *localServer {
	t.Helper()
	sk := newKey(t)
	ep, err := Bind(ctx, ServerConfig{Key: sk, ALPN: testALPN, Local: true, Log: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	srv := &localServer{ep: ep, key: sk, peers: make(chan irohkey.EndpointID, 16)}

	serveCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ep.Accept(serveCtx)
			if err != nil {
				return
			}
			srv.peers <- conn.RemoteID()
			go echoStreams(serveCtx, conn)
		}
	}()
	t.Cleanup(func() {
		stop()
		<-done
		ep.Shutdown(context.Background())
	})
	return srv
}

func echoStreams(ctx context.Context, conn *iroh.Conn) {
	for {
		stream, err := conn.AcceptStreamConn(ctx)
		if err != nil {
			return
		}
		go func() {
			defer stream.Close()
			msg := make([]byte, messageSize)
			if _, err := io.ReadFull(stream, msg); err != nil {
				return
			}
			stream.Write(append([]byte("re:"), msg...))
		}()
	}
}

func (s *localServer) addr() netip.AddrPort { return s.ep.LocalAddr() }

func (s *localServer) id() irohkey.EndpointID { return s.key.Public().EndpointID() }

// exchange sends msg on a new stream of conn and returns the answer.
func exchange(ctx context.Context, conn *Conn, msg string) (string, error) {
	if len(msg) != messageSize {
		return "", fmt.Errorf("message of %d bytes, want %d", len(msg), messageSize)
	}
	stream, err := conn.OpenStreamConn(ctx)
	if err != nil {
		return "", fmt.Errorf("open stream: %w", err)
	}
	defer stream.Close()
	if deadline, ok := ctx.Deadline(); ok {
		stream.SetDeadline(deadline)
	}
	if _, err := stream.Write([]byte(msg)); err != nil {
		return "", fmt.Errorf("write: %w", err)
	}
	answer := make([]byte, len("re:")+messageSize)
	if _, err := io.ReadFull(stream, answer); err != nil {
		return "", fmt.Errorf("read: %w", err)
	}
	return string(answer), nil
}

func TestLocalServerAndClientExchangeMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	srv := startLocalServer(t, ctx)

	if addr := srv.addr(); !addr.Addr().IsLoopback() || !addr.Addr().Is4() || addr.Port() == 0 {
		t.Fatalf("local server is bound to %v, want a port on 127.0.0.1", addr)
	}

	// The context of the dial ends as soon as the dial returns: the
	// connection must not depend on it.
	dialCtx, stopDial := context.WithCancel(ctx)
	clientKey := newKey(t)
	conn, err := Dial(dialCtx, DialConfig{
		Key:    clientKey,
		ALPN:   testALPN,
		Server: srv.id(),
		Addrs:  []netip.AddrPort{srv.addr()},
		Log:    testLogger(t),
	})
	stopDial()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if got := conn.RemoteID(); got != srv.id() {
		t.Fatalf("client sees server ID %v, want %v", got, srv.id())
	}
	select {
	case got := <-srv.peers:
		if want := clientKey.Public().EndpointID(); got != want {
			t.Fatalf("server sees client ID %v, want %v", got, want)
		}
	case <-ctx.Done():
		t.Fatal("server accepted no connection")
	}

	got, err := exchange(ctx, conn, "message1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "re:message1" {
		t.Fatalf("answer %q, want %q", got, "re:message1")
	}
}

func TestTwoRequestsOnTwoStreamsOfOneConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	srv := startLocalServer(t, ctx)

	conn, err := Dial(ctx, DialConfig{
		Key:    newKey(t),
		ALPN:   testALPN,
		Server: srv.id(),
		Addrs:  []netip.AddrPort{srv.addr()},
		Log:    testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	first, err := conn.OpenStreamConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := conn.OpenStreamConn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	deadline, _ := ctx.Deadline()
	first.SetDeadline(deadline)
	second.SetDeadline(deadline)

	// The second stream is answered while the first is still open and has
	// not been written to, so the two are independent of each other.
	if _, err := second.Write([]byte("request2")); err != nil {
		t.Fatal(err)
	}
	answer := make([]byte, len("re:")+messageSize)
	if _, err := io.ReadFull(second, answer); err != nil {
		t.Fatal(err)
	}
	if string(answer) != "re:request2" {
		t.Fatalf("second stream answered %q", answer)
	}
	if _, err := first.Write([]byte("request1")); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(first, answer); err != nil {
		t.Fatal(err)
	}
	if string(answer) != "re:request1" {
		t.Fatalf("first stream answered %q", answer)
	}

	select {
	case <-srv.peers:
	case <-ctx.Done():
		t.Fatal("server accepted no connection")
	}
	select {
	case id := <-srv.peers:
		t.Fatalf("server accepted a second connection, from %v", id)
	default:
	}

	// A third request, after two streams have come and gone.
	got, err := exchange(ctx, conn, "request3")
	if err != nil {
		t.Fatal(err)
	}
	if got != "re:request3" {
		t.Fatalf("third stream answered %q", got)
	}
}

// A connection gives its peer credit for a limited number of open streams
// and renews it as streams are retired. A stream that is closed and not
// retired would use the credit up, and the open of a later stream would
// block.
func TestManyStreamsInSuccessionOnOneConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	srv := startLocalServer(t, ctx)

	conn, err := Dial(ctx, DialConfig{
		Key:    newKey(t),
		ALPN:   testALPN,
		Server: srv.id(),
		Addrs:  []netip.AddrPort{srv.addr()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for i := range 500 {
		msg := fmt.Sprintf("req.%04d", i)
		got, err := exchange(ctx, conn, msg)
		if err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
		if got != "re:"+msg {
			t.Fatalf("stream %d answered %q", i, got)
		}
	}
}

func TestDialConnectsPastAnAddressThatNeverAnswers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	srv := startLocalServer(t, ctx)

	started := time.Now()
	conn, err := Dial(ctx, DialConfig{
		Key:    newKey(t),
		ALPN:   testALPN,
		Server: srv.id(),
		Addrs:  []netip.AddrPort{silentAddr(t), srv.addr(), silentAddr(t)},
		Log:    testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if took := time.Since(started); took > 5*time.Second {
		t.Fatalf("dial took %v: the dead addresses were waited for", took)
	}

	got, err := exchange(ctx, conn, "raced..1")
	if err != nil {
		t.Fatal(err)
	}
	if got != "re:raced..1" {
		t.Fatalf("answer %q", got)
	}
}

func TestDialFailsWhenNoAddressAnswers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	dialCtx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	conn, err := Dial(dialCtx, DialConfig{
		Key:    newKey(t),
		ALPN:   testALPN,
		Server: newKey(t).Public().EndpointID(),
		Addrs:  []netip.AddrPort{silentAddr(t)},
		Log:    testLogger(t),
	})
	if err == nil {
		conn.Close()
		t.Fatal("dial of an address that never answers succeeded")
	}
	if ctx.Err() != nil {
		t.Fatalf("dial did not return when its context ended: %v", err)
	}
}

func TestDialRefusesAServerWithAnotherKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	srv := startLocalServer(t, ctx)

	dialCtx, stop := context.WithTimeout(ctx, 2*time.Second)
	defer stop()
	conn, err := Dial(dialCtx, DialConfig{
		Key:    newKey(t),
		ALPN:   testALPN,
		Server: newKey(t).Public().EndpointID(),
		Addrs:  []netip.AddrPort{srv.addr()},
		Log:    testLogger(t),
	})
	if err == nil {
		conn.Close()
		t.Fatal("connected to a server that does not hold the key of the ID that was dialed")
	}
}

func TestCloseIsSafeToRepeatAndEndsStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	srv := startLocalServer(t, ctx)

	conn, err := Dial(ctx, DialConfig{
		Key:    newKey(t),
		ALPN:   testALPN,
		Server: srv.id(),
		Addrs:  []netip.AddrPort{srv.addr()},
		Log:    testLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := exchange(ctx, conn, "too.late"); err == nil {
		t.Fatal("a stream was opened and answered on a closed connection")
	}
}

func TestConfigurationErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	sk := newKey(t)
	server := newKey(t).Public().EndpointID()
	addrs := []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:1")}

	for name, cfg := range map[string]ServerConfig{
		"no key":  {ALPN: testALPN, Local: true},
		"no ALPN": {Key: sk, Local: true},
	} {
		t.Run("bind/"+name, func(t *testing.T) {
			ep, err := Bind(ctx, cfg)
			if err == nil {
				ep.Shutdown(context.Background())
				t.Fatal("no error")
			}
		})
	}
	for name, cfg := range map[string]DialConfig{
		"no key":    {ALPN: testALPN, Server: server, Addrs: addrs},
		"no ALPN":   {Key: sk, Server: server, Addrs: addrs},
		"no server": {Key: sk, ALPN: testALPN, Addrs: addrs},
		"own ID":    {Key: sk, ALPN: testALPN, Server: sk.Public().EndpointID(), Addrs: addrs},
	} {
		t.Run("dial/"+name, func(t *testing.T) {
			conn, err := Dial(ctx, cfg)
			if err == nil {
				conn.Close()
				t.Fatal("no error")
			}
		})
	}
}

func TestNoLogLineCarriesTheKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	var logged bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug}))

	serverKey, clientKey := newKey(t), newKey(t)
	ep, err := Bind(ctx, ServerConfig{Key: serverKey, ALPN: testALPN, Local: true, Log: log})
	if err != nil {
		t.Fatal(err)
	}
	defer ep.Shutdown(context.Background())
	// The server waits for the connection to end, which it learns from
	// the client's Close and not from a timeout.
	accepted := make(chan error, 1)
	go func() {
		conn, err := ep.Accept(ctx)
		if err == nil {
			select {
			case <-conn.Context().Done():
			case <-ctx.Done():
				err = errors.New("the server did not see the client close the connection")
			}
			conn.Close()
		}
		accepted <- err
	}()

	conn, err := Dial(ctx, DialConfig{
		Key:    clientKey,
		ALPN:   testALPN,
		Server: serverKey.Public().EndpointID(),
		Addrs:  []netip.AddrPort{ep.LocalAddr()},
		Log:    log,
	})
	if err != nil {
		t.Fatal(err)
	}
	closed := time.Now()
	conn.Close()
	if err := <-accepted; err != nil {
		t.Fatalf("accept: %v", err)
	}
	if took := time.Since(closed); took > 5*time.Second {
		t.Fatalf("the server saw the connection end %v after the client closed it", took)
	}

	for _, sk := range []irohkey.SecretKey{serverKey, clientKey} {
		seed := sk.Bytes()
		if strings.Contains(logged.String(), fmt.Sprintf("%x", seed[:])) {
			t.Fatal("a log line carries a secret key")
		}
	}
	if logged.Len() == 0 {
		t.Fatal("nothing was logged, so the check above proves nothing")
	}
}

func newKey(t *testing.T) irohkey.SecretKey {
	t.Helper()
	sk, err := irohkey.GenerateSecretKey()
	if err != nil {
		t.Fatal(err)
	}
	return sk
}

// silentAddr returns a loopback address with a socket behind it that
// never answers, as a firewall that drops packets would look to a dialer.
func silentAddr(t *testing.T) netip.AddrPort {
	t.Helper()
	sock, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sock.Close() })
	return sock.LocalAddr().(*net.UDPAddr).AddrPort()
}

func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// testWriter writes log lines to the log of a test.
type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Helper()
	w.t.Log(strings.TrimRight(string(p), "\n"))
	return len(p), nil
}

// A server that names the address to bind gets that address: a firewall
// rule is written for a port, not for whatever the system picked.
func TestBindTakesTheAddressItIsGiven(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	// A port that was free a moment ago.
	probe, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	want := probe.LocalAddr().(*net.UDPAddr).AddrPort()
	probe.Close()

	ep, err := Bind(ctx, ServerConfig{Key: newKey(t), ALPN: testALPN, Local: true, Bind: want, Log: testLogger(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer ep.Shutdown(context.Background())
	if got := ep.LocalAddr(); got != want {
		t.Fatalf("bound to %s, want %s", got, want)
	}

	// The port is taken now, and a second server asking for it is told.
	if second, err := Bind(ctx, ServerConfig{Key: newKey(t), ALPN: testALPN, Local: true, Bind: want, Log: testLogger(t)}); err == nil {
		second.Shutdown(context.Background())
		t.Fatal("a second endpoint was bound to the same address")
	}
}
