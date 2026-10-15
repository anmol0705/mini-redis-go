package server

import (
	"strings"

	"github.com/anmol0705/mini-redis-go/internal/resp"
)

// session is the state of one client connection, owned by that
// connection's goroutine (so it needs no lock).
//
// It sits between the network loop and the Handler. Commands that are about
// the connection itself, not the data, are answered here: QUIT today, and
// MULTI/EXEC/DISCARD and SUBSCRIBE in later phases, which need per-connection
// state such as a queue of commands or a subscription list. Everything else
// is passed to the Handler. See docs/decisions/002-session-layer.md.
type session struct {
	h Handler
}

func newSession(h Handler) *session { return &session{h: h} }

// execute runs one command. closeAfter is true when the connection should
// be closed once the reply has been sent.
func (s *session) execute(args []string) (reply resp.Value, closeAfter bool) {
	if len(args) > 0 && strings.EqualFold(args[0], "QUIT") {
		return resp.SimpleString("OK"), true
	}
	return s.h.Execute(args), false
}
