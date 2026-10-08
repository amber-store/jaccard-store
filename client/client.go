// Package client is the client side of jaccard-store: it pushes references
// from a local Amber-Store Core packstore to a server and pulls them back.
//
// The client talks to the server over streams of one iroh connection
// (package wire) and to the bucket over plain HTTP, through the pre-signed
// URLs the server hands out. Pack bytes never pass through the server.
//
// A push sends the sketch of the reference's key set, learns which base
// packs are near, and uploads either a patch pack against the best of them
// or a base pack. A pull imports the reference's pack and fetches the
// parent only when objects are still missing after that.
package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"

	"github.com/amber-store/core/key"
	"github.com/amber-store/jaccard-store/wire"
)

// ErrNotFound is returned by Pull and Delete for a name the server has no
// reference under.
var ErrNotFound = errors.New("client: no such reference")

// Opener opens one stream per request. *node.Conn and *iroh.Conn do.
type Opener interface {
	OpenStreamConn(ctx context.Context) (net.Conn, error)
}

// Client is a connection to a server. It is safe for concurrent use.
type Client struct {
	conn Opener
	http *http.Client
}

// New returns a client that sends its requests over conn and reaches the
// bucket through hc. A nil hc means a client without a timeout of its own:
// a transfer lasts as long as its context lets it.
func New(conn Opener, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{}
	}
	return &Client{conn: conn, http: hc}
}

// call sends one request and returns the answer. A refusal comes back as
// the *wire.Error the server sent.
func (c *Client) call(ctx context.Context, req wire.Request) (wire.Response, error) {
	stream, err := c.conn.OpenStreamConn(ctx)
	if err != nil {
		return wire.Response{}, fmt.Errorf("%s: opening a stream: %w", req.Op, err)
	}
	defer stream.Close()
	// A stream knows nothing of ctx: closing it is what ends a wait.
	stop := context.AfterFunc(ctx, func() { stream.Close() })
	defer stop()

	if err := wire.WriteFrame(stream, req); err != nil {
		return wire.Response{}, fmt.Errorf("%s: %w", req.Op, orCause(ctx, err))
	}
	if half, ok := stream.(interface{ CloseWrite() error }); ok {
		half.CloseWrite()
	}
	var resp wire.Response
	if err := wire.ReadFrame(stream, &resp); err != nil {
		return wire.Response{}, fmt.Errorf("%s: reading the answer: %w", req.Op, orCause(ctx, err))
	}
	if resp.Error != nil {
		return wire.Response{}, fmt.Errorf("%s: %w", req.Op, resp.Error)
	}
	return resp, nil
}

// orCause prefers the end of ctx over the error it caused on a stream.
func orCause(ctx context.Context, err error) error {
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	return err
}

// refused reports whether err is the server's refusal with code.
func refused(err error, code string) bool {
	var werr *wire.Error
	return errors.As(err, &werr) && werr.Code == code
}

// Ref is a reference on the server: a name and the root key it points at.
type Ref struct {
	Name string
	Root key.Key
}

// List returns the server's references whose names begin with prefix, in
// name order.
func (c *Client) List(ctx context.Context, prefix string) ([]Ref, error) {
	var refs []Ref
	after := ""
	for {
		resp, err := c.call(ctx, wire.Request{Op: wire.OpList, Prefix: prefix, After: after})
		if err != nil {
			return nil, err
		}
		for _, r := range resp.Refs {
			root, err := key.Parse(r.Root)
			if err != nil {
				return nil, fmt.Errorf("list: reference %q: %w", r.Name, err)
			}
			refs = append(refs, Ref{Name: r.Name, Root: root})
		}
		if !resp.More || len(resp.Refs) == 0 {
			return refs, nil
		}
		after = resp.Refs[len(resp.Refs)-1].Name
	}
}

// Delete removes the reference name from the server. The packs it leaves
// without a reference are collected there.
func (c *Client) Delete(ctx context.Context, name string) error {
	_, err := c.call(ctx, wire.Request{Op: wire.OpDelete, Name: name})
	if refused(err, wire.CodeNotFound) {
		return fmt.Errorf("%w: %q", ErrNotFound, name)
	}
	return err
}
