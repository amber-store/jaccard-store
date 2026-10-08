package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/amber-store/jaccard-store/client"
	"github.com/urfave/cli/v2"
)

// ls and rm take patterns: any number of them, and a reference that
// several match is listed or removed once. A pattern is a name as it is, or
// one with
//
//	*       any run of characters, none included
//	?       any one character
//	[a-c]   any one of these characters; [^a-c] any one but these
//	\x      the character x, for a name that holds one of the above
//
// A slash is a character like any other: to the server a name is a string
// and nothing more. So "releases/*" is everything under releases/, however
// deep.
//
// The two commands read an argument that has none of these characters
// differently. To rm it is a name, and that reference alone is removed. To
// ls it is what a name begins with, as it was before ls took patterns:
// "ls releases/" lists what "ls 'releases/*'" does. Listing by what a name
// begins with is how one looks around; removing that way would take more
// than was named.

// metas are the characters that make an argument more than a name.
const metas = `*?[\`

// checkPatterns reports the first argument that is no pattern. It is asked
// before the server is dialed: an argument that cannot be read should not
// get that far.
func checkPatterns(patterns []string) error {
	for _, p := range patterns {
		if _, err := path.Match(p, ""); err != nil {
			return fmt.Errorf("%q is not a pattern: %w", p, err)
		}
	}
	return nil
}

// matching asks the server for the references pattern matches. A pattern
// without a special character matches the reference of that name; with
// prefix it stands for every reference whose name begins with it.
func matching(ctx context.Context, server remote, pattern string, prefix bool) ([]client.Ref, error) {
	// The server narrows the list down to the names that begin as the
	// pattern does; the rest of the pattern is held against them here.
	listed, err := server.List(ctx, literalPrefix(pattern))
	if err != nil {
		return nil, err
	}
	if prefix && !strings.ContainsAny(pattern, metas) {
		return listed, nil
	}
	return slices.DeleteFunc(listed, func(r client.Ref) bool { return !matches(pattern, r.Name) }), nil
}

// found collects references by name: one that several patterns match is
// there once.
type found map[string]client.Ref

func (f found) add(refs []client.Ref) {
	for _, r := range refs {
		f[r.Name] = r
	}
}

// sorted returns the references in the order of their names.
func (f found) sorted() []client.Ref {
	refs := make([]client.Ref, 0, len(f))
	for _, r := range f {
		refs = append(refs, r)
	}
	slices.SortFunc(refs, func(a, b client.Ref) int { return cmp.Compare(a.Name, b.Name) })
	return refs
}

// printRef writes a reference as ls does: its name and its root to a line.
func printRef(c *cli.Context, r client.Ref) error {
	_, err := fmt.Fprintf(c.App.Writer, "%s %s\n", r.Name, r.Root)
	return err
}

func runList(c *cli.Context, connect dialer) error {
	patterns := c.Args().Slice()
	if err := checkPatterns(patterns); err != nil {
		return err
	}
	server, err := connect(c.Context, readSettings(c))
	if err != nil {
		return err
	}
	defer server.Close()
	// Without an argument everything is listed: what every name begins
	// with is nothing.
	if len(patterns) == 0 {
		patterns = []string{""}
	}
	refs := found{}
	for _, pattern := range patterns {
		// A pattern that matches nothing lists nothing, and that is no
		// failure: there is nothing of the kind, which is an answer.
		matched, err := matching(c.Context, server, pattern, true)
		if err != nil {
			return err
		}
		refs.add(matched)
	}
	for _, r := range refs.sorted() {
		if err := printRef(c, r); err != nil {
			return err
		}
	}
	return nil
}

func runRemove(c *cli.Context, connect dialer) error {
	if c.NArg() == 0 {
		return errors.New("rm takes one or more arguments PATTERN, got none")
	}
	patterns := c.Args().Slice()
	if err := checkPatterns(patterns); err != nil {
		return err
	}
	server, err := connect(c.Context, readSettings(c))
	if err != nil {
		return err
	}
	defer server.Close()

	// Everything is looked up before anything is removed. An argument
	// that matches nothing is an error, and nothing is removed for the
	// others then: a mistyped argument should not take the half of what
	// was meant that happened to be spelled right.
	refs := found{}
	for _, pattern := range patterns {
		matched, err := matching(c.Context, server, pattern, false)
		switch {
		case err != nil:
			return err
		case len(matched) > 0:
		case strings.ContainsAny(pattern, metas):
			return fmt.Errorf("no reference matches %q", pattern)
		default:
			return fmt.Errorf("%w: %q", client.ErrNotFound, pattern)
		}
		refs.add(matched)
	}
	for _, r := range refs.sorted() {
		err := server.Delete(c.Context, r.Name)
		// Removed by somebody else since it was listed: gone is what it
		// was to be.
		if errors.Is(err, client.ErrNotFound) {
			continue
		}
		if err != nil {
			return fmt.Errorf("removing %q: %w", r.Name, err)
		}
		// As ls prints it, so that what is gone can be read off, and
		// pushed again by whoever still has it.
		if err := printRef(c, r); err != nil {
			return err
		}
	}
	return nil
}

// matches reports whether name is one the pattern stands for.
//
// The matching is path.Match's, but for the slash, which stops a star
// there and must not here. So the slashes are taken out of its sight: no
// name holds a control character, and one stands in for the slash on both
// sides.
func matches(pattern, name string) bool {
	const slash = "\x00"
	ok, _ := path.Match(strings.ReplaceAll(pattern, "/", slash), strings.ReplaceAll(name, "/", slash))
	return ok
}

// literalPrefix is what every name a pattern matches begins with: the
// pattern up to its first character that is more than itself.
func literalPrefix(pattern string) string {
	if i := strings.IndexAny(pattern, metas); i >= 0 {
		return pattern[:i]
	}
	return pattern
}
