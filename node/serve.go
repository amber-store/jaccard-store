// Package node is where jaccard-store meets iroh: the key file, the
// server's endpoint with its announcing, and the client's dial.
//
// A server is found by its endpoint ID alone, the public half of the key in
// its key file. Bind makes that so. It binds the endpoint with the default
// relays and announces it two ways: a pkarr record with number0's DNS
// server, holding the home relay and the direct addresses, for clients
// anywhere, and mDNS, holding the direct addresses, for clients on the same
// link, which then need no internet. The direct addresses are those of the
// machine's interfaces on the bound port, because the socket is bound to
// the wildcard address and that is nothing a client can dial.
//
// A client dials with Dial. It asks mDNS, pkarr and DNS for the ID, takes
// every address any of them answers with, and starts a connection to each
// at once; the first that completes is the connection. A direct address
// wins where one is reachable, and the relay is among the candidates, so it
// is the path that remains when none is.
//
// Tests need neither the internet nor the link: a server bound with
// ServerConfig.Local listens on 127.0.0.1 and announces nothing, and a
// client given DialConfig.Addrs dials those addresses and asks nobody.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"strings"
	"time"

	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/iroh/mdns"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
)

// onlineWait is how long Bind waits for the connection to the home relay.
const onlineWait = 15 * time.Second

// ServerConfig configures the endpoint of a server.
type ServerConfig struct {
	// Key is the identity of the server. Its public half is the endpoint
	// ID that clients dial.
	Key irohkey.SecretKey
	// ALPN is the protocol the endpoint accepts connections for.
	ALPN string
	// Bind is the UDP address the endpoint binds. The zero value leaves it
	// to go-iroh: every address of both families, on a port the system
	// picks. A server behind a firewall, or on the host network of a
	// cluster, names a port here so that a rule can be written for it. An
	// unspecified address binds every address of its family, so 0.0.0.0
	// serves IPv4 alone.
	Bind netip.AddrPort
	// Local is for tests: the endpoint uses no relay and is announced
	// nowhere, and is bound to 127.0.0.1 unless Bind says otherwise.
	// Clients dial Endpoint.LocalAddr.
	Local bool
	// Log receives what Bind and the announcing have to say. Nil means
	// slog.Default().
	Log *slog.Logger
}

// Bind binds the server's endpoint. Unless cfg.Local it waits for the
// relay (15 s at most, then warns), advertises the interface addresses,
// and announces through pkarr and mDNS until ctx is done.
//
// The pkarr record follows the endpoint: it is published again when the
// home relay or the direct addresses change. Publishing runs in the
// background, and go-iroh retries a publish that fails without reporting
// it, so neither Bind nor the log can say that the record is being served.
// A machine without mDNS is warned about and served without it. The
// announcing ends when the endpoint is shut down as well as when ctx is
// done.
//
// The caller accepts the connections and shuts the endpoint down.
func Bind(ctx context.Context, cfg ServerConfig) (*iroh.Endpoint, error) {
	if cfg.Key.IsZero() {
		return nil, errors.New("bind: no key")
	}
	if cfg.ALPN == "" {
		return nil, errors.New("bind: no ALPN")
	}
	log := logger(cfg.Log)
	opts := []iroh.Option{iroh.WithSecretKey(cfg.Key), iroh.WithALPNs(cfg.ALPN)}

	if cfg.Local {
		addr := cfg.Bind
		if !addr.IsValid() {
			addr = netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), 0)
		}
		ep, err := iroh.Bind(ctx, append(opts, iroh.WithRelayMode(relay.ModeDisabled()), iroh.WithBindAddr(addr))...)
		if err != nil {
			return nil, fmt.Errorf("bind: %w", err)
		}
		log.Info("endpoint bound for local use", "id", ep.ID(), "addr", ep.LocalAddr())
		return ep, nil
	}

	opts = append(opts, iroh.WithRelayMode(relay.ModeDefault()))
	if cfg.Bind.IsValid() {
		opts = append(opts, iroh.WithBindAddr(cfg.Bind))
	}
	ep, err := iroh.Bind(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("bind: %w", err)
	}
	if err := announce(ctx, ep, cfg.Key, log); err != nil {
		ep.Shutdown(context.Background())
		return nil, fmt.Errorf("bind: %w", err)
	}
	return ep, nil
}

// announce makes ep reachable by its ID: it waits for the relay, pins the
// interface addresses as direct addresses, and starts the mDNS responder
// and the pkarr publisher, which run until ctx is done or ep is closed.
func announce(ctx context.Context, ep *iroh.Endpoint, sk irohkey.SecretKey, log *slog.Logger) error {
	onlineCtx, cancel := context.WithTimeout(ctx, onlineWait)
	err := ep.Online(onlineCtx)
	cancel()
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		log.Warn("no relay connection yet; until there is one, only clients that reach a direct address can connect",
			"waited", onlineWait, "error", err)
	}

	ifaces, err := localIfaceAddrs()
	if err != nil {
		return fmt.Errorf("interface addresses: %w", err)
	}
	direct := directAddrs(ifaces, ep.LocalAddr())
	directAddrs := make([]netaddr.TransportAddr, 0, len(direct))
	for _, ap := range direct {
		ep.AddExternalAddr(ap)
		directAddrs = append(directAddrs, netaddr.IPAddr{Addr: ap})
	}

	pub, err := iroh.N0PkarrPublisher(sk, &iroh.PkarrPublisherConfig{AddrFilter: publishableAddrs})
	if err != nil {
		return fmt.Errorf("pkarr publisher: %w", err)
	}
	ctx = untilClosed(ctx, ep)

	local := mdns.New(ep.ID(), mdns.WithLogger(log))
	if len(directAddrs) > 0 {
		local.Publish(dns.NewEndpointData(directAddrs...))
	} else {
		log.Warn("no interface address to advertise; the server is reachable through the relay only")
	}
	go func() {
		if err := local.Start(ctx); err != nil && ctx.Err() == nil {
			log.Warn("mDNS is unavailable; clients on the local network find the server through pkarr and DNS only", "error", err)
		}
	}()

	go func() {
		defer pub.Close()
		for addr := range ep.WatchAddr().Stream(ctx) {
			addrs := publishableAddrs(addr.Addrs())
			pub.Publish(dns.NewEndpointData(addrs...))
			log.Info("publishing endpoint record", "id", ep.ID(), "addrs", addrs)
		}
	}()
	return nil
}

// untilClosed returns a context that is done when ctx is done or ep is
// closed, whichever comes first.
func untilClosed(ctx context.Context, ep *iroh.Endpoint) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		select {
		case <-ep.Closed():
		case <-ctx.Done():
		}
	}()
	return ctx
}

// logger returns l, or the default logger when l is nil.
func logger(l *slog.Logger) *slog.Logger {
	if l == nil {
		return slog.Default()
	}
	return l
}

// ifaceAddrs is the name, the state and the addresses of one network
// interface: what advertisedAddrPorts decides on, so that it can be tested
// without real interfaces.
type ifaceAddrs struct {
	name  string
	up    bool
	addrs []net.Addr
}

// bridgePrefixes begin the names of the interfaces of containers and
// virtual machines. Their addresses cannot be reached from another machine,
// and every dead address that is advertised is one more a client waits on.
var bridgePrefixes = []string{"docker", "br-", "cni", "flannel", "veth", "virbr", "lxc"}

func isBridgeName(name string) bool {
	for _, p := range bridgePrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// advertisedAddrPorts pairs the unicast addresses of the machine's
// interfaces with the port the endpoint is bound to: the direct addresses
// worth advertising. Interfaces that are down, container bridges, and
// loopback, link-local and unspecified addresses are left out. An IPv4
// address written as IPv6 is advertised as IPv4. No address is listed
// twice, and the order of the interfaces is kept.
func advertisedAddrPorts(ifaces []ifaceAddrs, port uint16) []netip.AddrPort {
	var out []netip.AddrPort
	seen := make(map[netip.Addr]bool)
	for _, ifc := range ifaces {
		if !ifc.up || isBridgeName(ifc.name) {
			continue
		}
		for _, a := range ifc.addrs {
			n, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip, ok := netip.AddrFromSlice(n.IP)
			if !ok {
				continue
			}
			ip = ip.Unmap()
			if seen[ip] || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
				continue
			}
			seen[ip] = true
			out = append(out, netip.AddrPortFrom(ip, port))
		}
	}
	return out
}

// directAddrs returns the addresses at which an endpoint bound to local can
// be dialed directly. A socket bound to one address is reached at that one
// and no other. A socket bound to every address is reached at the addresses
// of the interfaces, of both families or, when it was bound as 0.0.0.0, of
// IPv4 alone. An address nobody can reach it at costs every client that
// tries it a part of its time to connect.
func directAddrs(ifaces []ifaceAddrs, local netip.AddrPort) []netip.AddrPort {
	addr := local.Addr().Unmap()
	if !addr.IsUnspecified() {
		if addr.IsLoopback() || addr.IsLinkLocalUnicast() {
			return nil
		}
		return []netip.AddrPort{netip.AddrPortFrom(addr, local.Port())}
	}
	all := advertisedAddrPorts(ifaces, local.Port())
	if !addr.Is4() {
		return all
	}
	var v4 []netip.AddrPort
	for _, ap := range all {
		if ap.Addr().Is4() {
			v4 = append(v4, ap)
		}
	}
	return v4
}

// localIfaceAddrs lists the machine's interfaces for advertisedAddrPorts.
// An interface whose addresses cannot be read is left out.
func localIfaceAddrs() ([]ifaceAddrs, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	out := make([]ifaceAddrs, 0, len(ifaces))
	for _, ifc := range ifaces {
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		out = append(out, ifaceAddrs{name: ifc.Name, up: ifc.Flags&net.FlagUp != 0, addrs: addrs})
	}
	return out, nil
}

// publishableAddrs is the address filter of the pkarr publisher: every
// address but the IP addresses nobody can dial, the invalid and the
// unspecified. The publisher's own default would publish the relay alone
// and drop the direct addresses that are advertised on purpose.
func publishableAddrs(addrs []netaddr.TransportAddr) []netaddr.TransportAddr {
	out := make([]netaddr.TransportAddr, 0, len(addrs))
	for _, a := range addrs {
		if ip, ok := a.(netaddr.IPAddr); ok && (!ip.Addr.IsValid() || ip.Addr.Addr().IsUnspecified()) {
			continue
		}
		out = append(out, a)
	}
	return out
}
