// Package command turns a parsed client command (a slice of strings) into
// a reply.
//
// Concurrency: a Dispatcher holds no mutable state of its own and every
// handler only touches the Store, which synchronises itself, so one
// Dispatcher may be used by all connection goroutines at once.
//
// Commands that belong to one connection rather than to the data (QUIT now;
// MULTI/EXEC and SUBSCRIBE later) are not handled here. They live in the
// server's per-connection session; see docs/decisions/002-session-layer.md.
package command

import (
	"fmt"
	"strings"

	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// Info is the runtime data INFO reports. It belongs to the server (uptime,
// client count, listening port), so the command package only describes the
// shape it needs.
type Info struct {
	Version          string
	TCPPort          int
	UptimeSeconds    int64
	ConnectedClients int
}

// InfoSource supplies Info on demand. The interface is declared here, where
// it is used, so the command package does not import the server package.
type InfoSource interface {
	Info() Info
}

// Dispatcher executes commands against a Store.
type Dispatcher struct {
	store *store.Store
	info  InfoSource
}

// New returns a Dispatcher. info may be nil, in which case INFO reports
// zero values for the server and clients sections.
func New(st *store.Store, info InfoSource) *Dispatcher {
	return &Dispatcher{store: st, info: info}
}

// handler runs one command. args[0] is the command name as the client typed it.
type handler func(d *Dispatcher, args []string) resp.Value

// spec describes one command. arity follows Redis: a positive number means
// exactly that many words (including the name); a negative number means at
// least that many.
type spec struct {
	name  string // lower-case, used in error messages
	arity int
	run   handler
}

// table maps upper-case command names to their spec. It is built once and
// never modified, so reading it from many goroutines needs no lock.
var table = map[string]spec{
	"PING":     {"ping", -1, cmdPing},
	"ECHO":     {"echo", 2, cmdEcho},
	"SET":      {"set", -3, cmdSet},
	"GET":      {"get", 2, cmdGet},
	"DEL":      {"del", -2, cmdDel},
	"EXISTS":   {"exists", -2, cmdExists},
	"DBSIZE":   {"dbsize", 1, cmdDBSize},
	"FLUSHALL": {"flushall", -1, cmdFlush},
	"FLUSHDB":  {"flushdb", -1, cmdFlush},
	"COMMAND":  {"command", -1, cmdCommand},
	"CLIENT":   {"client", -2, cmdClient},
	"CONFIG":   {"config", -2, cmdConfig},
	"INFO":     {"info", -1, cmdInfo},
}

// maxEchoLen limits how much client text we copy into an error message.
const maxEchoLen = 64

// Execute runs one command and returns its reply. It never panics on bad
// input: unknown commands and wrong argument counts become error replies.
func (d *Dispatcher) Execute(args []string) resp.Value {
	if len(args) == 0 {
		return resp.Error("ERR empty command")
	}
	s, ok := table[strings.ToUpper(args[0])]
	if !ok {
		return resp.Error(unknownCommand(args))
	}
	if (s.arity > 0 && len(args) != s.arity) || (s.arity < 0 && len(args) < -s.arity) {
		return wrongArgs(s.name)
	}
	return s.run(d, args)
}

func wrongArgs(name string) resp.Value {
	return resp.Error(fmt.Sprintf("ERR wrong number of arguments for '%s' command", name))
}

func syntaxError() resp.Value { return resp.Error("ERR syntax error") }

// quote escapes client text for use inside an error message. %q turns CR, LF
// and other control bytes into visible escapes, so a client cannot craft
// a command name that forges extra protocol lines; the cut keeps a huge
// argument from being echoed back in full.
func quote(s string) string {
	if len(s) > maxEchoLen {
		return fmt.Sprintf("%q...", s[:maxEchoLen])
	}
	return fmt.Sprintf("%q", s)
}

func unknownCommand(args []string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "ERR unknown command %s, with args beginning with:", quote(args[0]))
	for i, a := range args[1:] {
		if i == 3 {
			break
		}
		b.WriteByte(' ')
		b.WriteString(quote(a))
	}
	return b.String()
}
