// Package testclient is a minimal RESP client for tests. It is deliberately
// independent of package resp's reader, so tests of the server do not depend
// on the code they are checking. It must not be imported by production code.
package testclient

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Timeout bounds every network wait; passing tests never reach it.
const Timeout = 10 * time.Second

// Reply is a decoded server reply.
type Reply struct {
	Kind  byte // '+', '-', ':', '$', '*'
	Str   string
	Int   int64
	Null  bool
	Elems []Reply
}

// String renders a reply compactly: +OK, -ERR..., :1, $text, $nil, *[a b].
func (r Reply) String() string {
	switch r.Kind {
	case '$':
		if r.Null {
			return "$nil"
		}
		return "$" + r.Str
	case '*':
		parts := make([]string, len(r.Elems))
		for i, e := range r.Elems {
			parts[i] = e.String()
		}
		return "*[" + strings.Join(parts, " ") + "]"
	case ':':
		return ":" + strconv.FormatInt(r.Int, 10)
	}
	return string(r.Kind) + r.Str
}

// Strings returns the string form of each element of an array reply.
func (r Reply) Strings() []string {
	out := make([]string, len(r.Elems))
	for i, e := range r.Elems {
		out[i] = e.Str
	}
	return out
}

// Client is one connection.
type Client struct {
	Conn net.Conn
	r    *bufio.Reader
}

// Dial connects to addr.
func Dial(addr string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, Timeout)
	if err != nil {
		return nil, err
	}
	return &Client{Conn: conn, r: bufio.NewReader(conn)}, nil
}

// MustDial connects and closes the connection when the test ends.
func MustDial(tb testing.TB, addr string) *Client {
	tb.Helper()
	c, err := Dial(addr)
	if err != nil {
		tb.Fatalf("dial %s: %v", addr, err)
	}
	tb.Cleanup(func() { _ = c.Conn.Close() })
	return c
}

// Close closes the connection.
func (c *Client) Close() error { return c.Conn.Close() }

// Encode returns the RESP encoding of a command.
func Encode(args ...string) []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b.Bytes()
}

// Write sends raw bytes.
func (c *Client) Write(p []byte) error {
	_ = c.Conn.SetWriteDeadline(time.Now().Add(Timeout))
	_, err := c.Conn.Write(p)
	return err
}

// Send sends one command without waiting for the reply.
func (c *Client) Send(args ...string) error { return c.Write(Encode(args...)) }

// Read reads one reply.
func (c *Client) Read() (Reply, error) {
	_ = c.Conn.SetReadDeadline(time.Now().Add(Timeout))
	return c.read()
}

func (c *Client) read() (Reply, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return Reply{}, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		return Reply{}, fmt.Errorf("empty reply line")
	}
	kind, rest := line[0], line[1:]
	switch kind {
	case '+', '-':
		return Reply{Kind: kind, Str: rest}, nil
	case ':':
		n, err := strconv.ParseInt(rest, 10, 64)
		return Reply{Kind: kind, Int: n, Str: rest}, err
	case '$':
		n, err := strconv.Atoi(rest)
		if err != nil {
			return Reply{}, err
		}
		if n < 0 {
			return Reply{Kind: kind, Null: true}, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return Reply{}, err
		}
		return Reply{Kind: kind, Str: string(buf[:n])}, nil
	case '*':
		n, err := strconv.Atoi(rest)
		if err != nil {
			return Reply{}, err
		}
		out := Reply{Kind: kind}
		for range n {
			e, err := c.read()
			if err != nil {
				return Reply{}, err
			}
			out.Elems = append(out.Elems, e)
		}
		return out, nil
	}
	return Reply{}, fmt.Errorf("unknown reply type in %q", line)
}

// Do sends a command and reads its reply.
func (c *Client) Do(args ...string) (Reply, error) {
	if err := c.Send(args...); err != nil {
		return Reply{}, err
	}
	return c.Read()
}

// Cmd sends a command and returns the reply as a string; any I/O error fails
// the test. Use only from the test goroutine.
func (c *Client) Cmd(tb testing.TB, args ...string) string {
	tb.Helper()
	r, err := c.Do(args...)
	if err != nil {
		tb.Fatalf("%v: %v", args, err)
	}
	return r.String()
}

// Reply sends a command and returns the decoded reply; I/O errors fail the test.
func (c *Client) Reply(tb testing.TB, args ...string) Reply {
	tb.Helper()
	r, err := c.Do(args...)
	if err != nil {
		tb.Fatalf("%v: %v", args, err)
	}
	return r
}

// MustRead reads one reply and returns it as a string.
func (c *Client) MustRead(tb testing.TB) string {
	tb.Helper()
	r, err := c.Read()
	if err != nil {
		tb.Fatalf("read reply: %v", err)
	}
	return r.String()
}

// ExpectClosed fails the test unless the server closes the connection.
func (c *Client) ExpectClosed(tb testing.TB) {
	tb.Helper()
	_ = c.Conn.SetReadDeadline(time.Now().Add(Timeout))
	_, err := c.r.ReadByte()
	if err == nil {
		tb.Fatal("connection still open; want it closed by the server")
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		tb.Fatal("timed out waiting for the server to close the connection")
	}
}
