package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/iroh/mdns"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

// mdnsLookupTimeout is how long a dial waits for an answer on the local
// link. A server on the link answers within a fraction of it; a client
// elsewhere waits it out once and goes by pkarr and DNS.
const mdnsLookupTimeout = time.Second

// lookupBudget is how long a dial waits for the lookups together. pkarr
// and DNS answer within a second where the internet is reachable; where it
// is not, a request may hang for far longer than a dial should take, and a
// client on the server's link has the answer of mDNS by then.
const lookupBudget = 10 * time.Second

// DialConfig configures the connection of a client to a server.
type DialConfig struct {
	// Key is the identity of the client. The server sees its public half
	// as the RemoteID of the connection.
	Key irohkey.SecretKey
	// ALPN is the protocol to connect for.
	ALPN string
	// Server is the endpoint ID of the server.
	Server irohkey.EndpointID
	// Addrs is for tests: the client dials these addresses, uses no relay
	// and looks nothing up.
	Addrs []netip.AddrPort
	// Log receives what the dial has to say, at debug level. Nil means
	// slog.Default().
	Log *slog.Logger
}

// Conn is a connection of a client to a server, with the endpoint that was
// bound for it.
type Conn struct {
	conn *iroh.Conn
	ep   *iroh.Endpoint
	// stop ends what was started for the lookups, which lives as long as
	// the connection: the endpoint may look the server up again.
	stop context.CancelFunc

	close    sync.Once
	closeErr error
}

// Dial resolves the server by its ID through mDNS, pkarr and DNS and
// connects, racing the candidates. Close ends the connection and the
// endpoint under it.
//
// ctx bounds the dial alone; the connection outlives it.
func Dial(ctx context.Context, cfg DialConfig) (*Conn, error) {
	switch {
	case cfg.Key.IsZero():
		return nil, errors.New("dial: no key")
	case cfg.ALPN == "":
		return nil, errors.New("dial: no ALPN")
	case cfg.Server.IsZero():
		return nil, errors.New("dial: no server ID")
	case cfg.Server == cfg.Key.Public().EndpointID():
		return nil, fmt.Errorf("dial: %s is the ID of the client's own key", cfg.Server)
	}
	log := logger(cfg.Log)

	lookups, stop := context.WithCancel(context.Background())
	var (
		ep    *iroh.Endpoint
		cands []netaddr.TransportAddr
		err   error
	)
	if len(cfg.Addrs) > 0 {
		ep, cands, err = bindDirect(ctx, cfg)
	} else {
		ep, cands, err = bindAndResolve(ctx, lookups, cfg, log)
	}
	if err != nil {
		stop()
		return nil, fmt.Errorf("dial %s: %w", cfg.Server, err)
	}

	log.Debug("connecting", "server", cfg.Server, "candidates", cands)
	conn, err := raceConnect(ctx, ep, cfg.Server, cfg.ALPN, cands)
	if err != nil {
		stop()
		ep.Shutdown(context.Background())
		return nil, fmt.Errorf("dial %s: connect: %w", cfg.Server, err)
	}
	log.Debug("connected", "server", cfg.Server, "addr", conn.RemoteAddr())
	return &Conn{conn: conn, ep: ep, stop: stop}, nil
}

// bindDirect binds an endpoint that reaches the addresses of cfg and
// nothing else: no relay and no lookups.
//
// go-iroh binds one socket for both families unless it is told an address,
// and macOS may give such a socket a port that an IPv4 socket of another
// process holds already; what arrives for that port over IPv4 then goes to
// the other socket, and the endpoint hears no answer. An IPv4 socket is
// given no port that another one has, so that is what is bound when there
// are IPv4 addresses alone to reach.
func bindDirect(ctx context.Context, cfg DialConfig) (*iroh.Endpoint, []netaddr.TransportAddr, error) {
	opts := []iroh.Option{iroh.WithSecretKey(cfg.Key), iroh.WithRelayMode(relay.ModeDisabled())}
	if ipv4Alone(cfg.Addrs) {
		opts = append(opts, iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv4Unspecified(), 0)))
	}
	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("bind: %w", err)
	}
	cands := make([]netaddr.TransportAddr, len(cfg.Addrs))
	for i, ap := range cfg.Addrs {
		cands[i] = netaddr.IPAddr{Addr: ap}
	}
	return ep, cands, nil
}

// ipv4Alone reports whether there are addresses and every one of them is
// an IPv4 address. An IPv4 address written as IPv6 does not count as one:
// it is sent to as it is written.
func ipv4Alone(addrs []netip.AddrPort) bool {
	for _, ap := range addrs {
		if !ap.Addr().Is4() {
			return false
		}
	}
	return len(addrs) > 0
}

// bindAndResolve binds an endpoint with the default relays and looks the
// server up through mDNS, pkarr and DNS. The mDNS listener runs until
// lookups is done; it asks on the local link and announces nothing of the
// client.
//
// It returns the union of what the lookups answered. One answer may be
// half of the picture: mDNS knows direct addresses and no relay, and any
// record may list addresses that cannot be reached from where the client
// is. With all of them as candidates the relay stays among them.
func bindAndResolve(ctx, lookups context.Context, cfg DialConfig, log *slog.Logger) (*iroh.Endpoint, []netaddr.TransportAddr, error) {
	pkarr, err := iroh.N0PkarrResolver(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("pkarr resolver: %w", err)
	}
	local := mdns.New(cfg.Key.Public().EndpointID(),
		mdns.WithPassive(true), mdns.WithLookupTimeout(mdnsLookupTimeout), mdns.WithLogger(log))
	go func() {
		if err := local.Start(lookups); err != nil && lookups.Err() == nil {
			log.Debug("mDNS is unavailable; looking the server up through pkarr and DNS only", "error", err)
		}
	}()
	var services iroh.AddressLookupServices
	services.AddResolver(local)
	services.AddResolver(pkarr)
	services.AddResolver(iroh.N0DNSAddressLookup(nil))

	ep, err := iroh.Bind(ctx,
		iroh.WithSecretKey(cfg.Key),
		iroh.WithAddressLookup(&services),
		iroh.WithRelayMode(relay.ModeDefault()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("bind: %w", err)
	}

	cands, err := lookup(ctx, &services, cfg.Server, lookupBudget, log)
	if err != nil {
		ep.Shutdown(context.Background())
		return nil, nil, err
	}
	return ep, cands, nil
}

// lookup asks every service for the addresses of id and returns the union
// of the answers. It waits until all of them have answered or failed, and
// no longer than budget: a service that has not answered by then is left
// out, so that one that hangs does not use up the time the dial has.
func lookup(ctx context.Context, services *iroh.AddressLookupServices, id irohkey.EndpointID, budget time.Duration, log *slog.Logger) ([]netaddr.TransportAddr, error) {
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	addr := netaddr.NewEndpointAddr(id)
	var lastErr error
	for item, err := range services.Resolve(ctx, id) {
		if err != nil {
			lastErr = err
			continue
		}
		log.Debug("server looked up", "source", item.Provenance(), "addrs", item.Addr().Addrs())
		addr = addr.WithAddrs(item.Addr().Addrs()...)
	}
	if addr.IsEmpty() {
		if lastErr != nil {
			return nil, fmt.Errorf("no address found: %w", lastErr)
		}
		return nil, errors.New("no address found")
	}
	return addr.Addrs(), nil
}

// raceConnect starts a connection to every candidate at once and returns
// the first that completes. The attempts that lose are canceled, and one
// that completes after the winner is closed.
//
// A record may list addresses that are dead from where the client is, the
// address of a container bridge or of another network. Dialed one after
// the other they use up the time the dial has before a live one is tried;
// dialed together, a live one answers in milliseconds.
func raceConnect(ctx context.Context, ep *iroh.Endpoint, id irohkey.EndpointID, alpn string, cands []netaddr.TransportAddr) (*iroh.Conn, error) {
	if len(cands) == 0 {
		return nil, errors.New("no candidate addresses")
	}
	type result struct {
		conn *iroh.Conn
		err  error
	}
	ctx, cancel := context.WithCancel(ctx)
	results := make(chan result, len(cands))
	for _, ta := range cands {
		go func() {
			conn, err := ep.Connect(ctx, netaddr.NewEndpointAddr(id, ta), alpn)
			if err != nil {
				err = fmt.Errorf("%s: %w", ta, err)
			}
			results <- result{conn, err}
		}()
	}
	var errs []error
	for pending := len(cands); pending > 0; pending-- {
		r := <-results
		if r.err != nil {
			errs = append(errs, r.err)
			continue
		}
		cancel()
		go func(late int) {
			for ; late > 0; late-- {
				if r := <-results; r.conn != nil {
					r.conn.Close()
				}
			}
		}(pending - 1)
		return r.conn, nil
	}
	cancel()
	return nil, errors.Join(errs...)
}

// OpenStreamConn opens a bidirectional stream on the connection. Closing
// the stream closes both of its directions.
func (c *Conn) OpenStreamConn(ctx context.Context) (net.Conn, error) {
	return c.conn.OpenStreamConn(ctx)
}

// RemoteID returns the endpoint ID of the server, as the handshake
// verified it.
func (c *Conn) RemoteID() irohkey.EndpointID {
	return c.conn.RemoteID()
}

// Close closes the connection and shuts down the endpoint that was bound
// for it. It is safe to call more than once.
func (c *Conn) Close() error {
	c.close.Do(func() {
		err := c.conn.Close()
		c.stop()
		c.closeErr = errors.Join(err, c.ep.Shutdown(context.Background()))
	})
	return c.closeErr
}
