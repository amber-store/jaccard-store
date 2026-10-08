package main

import (
	"cmp"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/amber-store/jaccard-store/client"
	"github.com/urfave/cli/v2"
)

// rm removes the references that its arguments match. An argument is a
// pattern: a name as it is, or one with
//
//	*       any run of characters, none included
//	?       any one character
//	[a-c]   any one of these characters; [^a-c] any one but these
//	\x      the character x, for a name that holds one of the above
//
// A slash is a character like any other: to the server a name is a string
// and nothing more, as it is to ls, whose prefix does not stop at a slash
// either. So "releases/*" is everything under releases/, however deep.

// metas are the characters that make an argument more than a name.
const metas = `*?[\`

func runRemove(c *cli.Context, connect dialer) error {
	if c.NArg() == 0 {
		return errors.New("rm takes one or more arguments PATTERN, got none")
	}
	patterns := c.Args().Slice()
	// Before the server is dialed: an argument that cannot be read should
	// not get that far.
	for _, p := range patterns {
		if _, err := path.Match(p, ""); err != nil {
			return fmt.Errorf("%q is not a pattern: %w", p, err)
		}
	}
	server, err := connect(c.Context, readSettings(c))
	if err != nil {
		return err
	}
	defer server.Close()
	refs, err := resolve(c, server, patterns)
	if err != nil {
		return err
	}
	for _, r := range refs {
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
		if _, err := fmt.Fprintf(c.App.Writer, "%s %s\n", r.Name, r.Root); err != nil {
			return err
		}
	}
	return nil
}

// resolve returns the references the patterns match, each of them once, in
// the order of their names.
//
// An argument that matches nothing is an error, and nothing is removed for
// the others then: a mistyped argument should not take the half of what was
// meant that happened to be spelled right.
func resolve(c *cli.Context, server remote, patterns []string) ([]client.Ref, error) {
	found := map[string]client.Ref{}
	for _, pattern := range patterns {
		// The server narrows the list down to the names that begin as the
		// pattern does; the rest of the pattern is held against them here.
		listed, err := server.List(c.Context, literalPrefix(pattern))
		if err != nil {
			return nil, err
		}
		matched := false
		for _, r := range listed {
			if matches(pattern, r.Name) {
				found[r.Name], matched = r, true
			}
		}
		switch {
		case matched:
		case strings.ContainsAny(pattern, metas):
			return nil, fmt.Errorf("no reference matches %q", pattern)
		default:
			return nil, fmt.Errorf("%w: %q", client.ErrNotFound, pattern)
		}
	}
	refs := make([]client.Ref, 0, len(found))
	for _, r := range found {
		refs = append(refs, r)
	}
	slices.SortFunc(refs, func(a, b client.Ref) int { return cmp.Compare(a.Name, b.Name) })
	return refs, nil
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
