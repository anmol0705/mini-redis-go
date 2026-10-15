package server

import (
	"context"
	"strconv"
	"strings"

	"github.com/anmol0705/mini-redis-go/internal/command"
	"github.com/anmol0705/mini-redis-go/internal/pubsub"
	"github.com/anmol0705/mini-redis-go/internal/resp"
)

// Limits on per-connection state, so a client cannot make us hold unbounded
// memory.
const (
	// maxQueuedCommands caps one MULTI block.
	maxQueuedCommands = 100_000
	// maxSubscriptions caps channels per connection.
	maxSubscriptions = 10_000
	// DefaultSubscriberQueue is the pub/sub queue length per connection.
	DefaultSubscriberQueue = 256
)

var (
	errExecWithoutMulti    = resp.Error("ERR EXEC without MULTI")
	errDiscardWithoutMulti = resp.Error("ERR DISCARD without MULTI")
	errNestedMulti         = resp.Error("ERR MULTI calls can not be nested")
	errExecAbort           = resp.Error("EXECABORT Transaction discarded because of previous errors.")
	errNotInTx             = resp.Error("ERR Command not allowed inside a transaction")
	errSubscribedMode      = resp.Error("ERR only SUBSCRIBE / UNSUBSCRIBE / PING / QUIT are allowed in this context")
	errNoPubSub            = resp.Error("ERR pub/sub is not available")
	errTooManyQueued       = resp.Error("ERR transaction too large")
	errTooManySubs         = resp.Error("ERR too many subscriptions")
	replyOK                = resp.SimpleString("OK")
	replyQueued            = resp.SimpleString("QUEUED")
)

// sessionCmd identifies the commands the session answers itself.
type sessionCmd int

const (
	cmdOther sessionCmd = iota
	cmdQuit
	cmdMulti
	cmdExec
	cmdDiscard
	cmdSubscribe
	cmdUnsubscribe
	cmdPing
	cmdSync
)

// classify recognises session-level commands without allocating (the name is
// compared case-insensitively in place).
func classify(name string) sessionCmd {
	switch len(name) {
	case 4:
		switch {
		case strings.EqualFold(name, "QUIT"):
			return cmdQuit
		case strings.EqualFold(name, "EXEC"):
			return cmdExec
		case strings.EqualFold(name, "PING"):
			return cmdPing
		}
	case 5:
		if strings.EqualFold(name, "MULTI") {
			return cmdMulti
		}
	case 6:
		if strings.EqualFold(name, "MRSYNC") {
			return cmdSync
		}
	case 7:
		if strings.EqualFold(name, "DISCARD") {
			return cmdDiscard
		}
	case 9:
		if strings.EqualFold(name, "SUBSCRIBE") {
			return cmdSubscribe
		}
	case 11:
		if strings.EqualFold(name, "UNSUBSCRIBE") {
			return cmdUnsubscribe
		}
	}
	return cmdOther
}

// session is the state of one client connection, owned by that
// connection's goroutine (so it needs no lock).
//
// It sits between the network loop and the Handler. Commands that are about
// the connection itself, not the data, are answered here: QUIT, MULTI/EXEC/
// DISCARD (which need a per-connection queue) and SUBSCRIBE/UNSUBSCRIBE (a
// per-connection subscription, which also restricts what the client may do).
// Everything else goes to the Handler. See docs/decisions/002-session-layer.md.
type session struct {
	h Handler

	// transactions
	inTx   bool
	dirty  bool // a queued command was invalid: EXEC must abort
	queued [][]string

	// pub/sub
	hub        *pubsub.Hub
	queueSize  int
	sub        *pubsub.Subscriber // created at the first SUBSCRIBE
	onSub      func(*pubsub.Subscriber)
	onOverflow func()

	sync *syncRequest // set by MRSYNC; the connection loop then hands over

	out []resp.Value // reused result slice (valid until the next execute)
}

// syncRequest is a parsed "MRSYNC <replid> <offset>".
type syncRequest struct {
	replID string
	offset int64
}

// takeSync returns and clears a pending replication request.
func (s *session) takeSync() *syncRequest {
	r := s.sync
	s.sync = nil
	return r
}

func (s *session) startSync(args []string) []resp.Value {
	if len(args) != 3 {
		return s.one(resp.Error("ERR wrong number of arguments for 'mrsync' command"))
	}
	offset, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil || offset < -1 || len(args[1]) > 64 {
		return s.one(resp.Error("ERR invalid replication id or offset"))
	}
	s.sync = &syncRequest{replID: args[1], offset: offset}
	s.out = s.out[:0]
	return s.out
}

func newSession(h Handler, hub *pubsub.Hub, queueSize int, onSubscribe func(*pubsub.Subscriber), onOverflow func()) *session {
	if queueSize <= 0 {
		queueSize = DefaultSubscriberQueue
	}
	return &session{h: h, hub: hub, queueSize: queueSize, onSub: onSubscribe, onOverflow: onOverflow}
}

// subscribed reports whether the client is in subscribed mode.
func (s *session) subscribed() bool {
	return s.sub != nil && s.hub != nil && len(s.hub.Channels(s.sub)) > 0
}

// close releases the session's subscriptions.
func (s *session) close() {
	if s.sub != nil {
		s.hub.UnsubscribeAll(s.sub)
	}
}

func (s *session) one(v resp.Value) []resp.Value {
	s.out = append(s.out[:0], v)
	return s.out
}

// execute runs one command and returns the replies to send (usually one).
// closeAfter is true when the connection should be closed once they are sent.
// The returned slice is only valid until the next call.
func (s *session) execute(ctx context.Context, args []string) (replies []resp.Value, closeAfter bool) {
	if len(args) == 0 {
		return s.one(resp.Error("ERR empty command")), false
	}
	kind := classify(args[0])

	if kind == cmdQuit {
		return s.one(replyOK), true
	}
	if kind == cmdSync {
		if s.inTx || s.subscribed() {
			return s.one(errNotInTx), false
		}
		return s.startSync(args), false
	}
	if s.subscribed() {
		switch kind {
		case cmdSubscribe:
			return s.subscribe(args), false
		case cmdUnsubscribe:
			return s.unsubscribe(args), false
		case cmdPing:
			msg := ""
			if len(args) > 1 {
				msg = args[1]
			}
			return s.one(resp.Array(resp.BulkString("pong"), resp.BulkString(msg))), false
		}
		return s.one(errSubscribedMode), false
	}

	switch kind {
	case cmdMulti:
		if s.inTx {
			return s.one(errNestedMulti), false
		}
		s.inTx, s.dirty, s.queued = true, false, nil
		return s.one(replyOK), false
	case cmdExec:
		return s.one(s.exec(ctx)), false
	case cmdDiscard:
		if !s.inTx {
			return s.one(errDiscardWithoutMulti), false
		}
		s.resetTx()
		return s.one(replyOK), false
	case cmdSubscribe:
		if s.inTx {
			s.dirty = true
			return s.one(errNotInTx), false
		}
		return s.subscribe(args), false
	case cmdUnsubscribe:
		if s.inTx {
			s.dirty = true
			return s.one(errNotInTx), false
		}
		return s.unsubscribe(args), false
	}

	if s.inTx {
		return s.one(s.queue(args)), false
	}
	return s.one(s.h.Execute(ctx, args)), false
}

func (s *session) resetTx() { s.inTx, s.dirty, s.queued = false, false, nil }

// queue validates a command at MULTI time. A command that is unknown or has
// the wrong number of arguments poisons the transaction, as in Redis.
func (s *session) queue(args []string) resp.Value {
	if _, errReply := command.Resolve(args); errReply.Kind == resp.KindError {
		s.dirty = true
		return errReply
	}
	if len(s.queued) >= maxQueuedCommands {
		s.dirty = true
		return errTooManyQueued
	}
	s.queued = append(s.queued, args)
	return replyQueued
}

// exec runs the queued commands as one atomic unit in the engine. Errors
// inside the transaction do not undo earlier commands (same as Redis); each
// command's reply, error or not, is returned in the array.
func (s *session) exec(ctx context.Context) resp.Value {
	if !s.inTx {
		return errExecWithoutMulti
	}
	queued, dirty := s.queued, s.dirty
	s.resetTx()
	if dirty {
		return errExecAbort
	}
	if len(queued) == 0 {
		return resp.Array()
	}
	return resp.Array(s.h.ExecuteAtomic(ctx, queued)...)
}

// subscribe handles SUBSCRIBE channel [channel ...]: one confirmation per channel.
func (s *session) subscribe(args []string) []resp.Value {
	if len(args) < 2 {
		return s.one(resp.Error("ERR wrong number of arguments for 'subscribe' command"))
	}
	if s.hub == nil {
		return s.one(errNoPubSub)
	}
	if s.sub == nil {
		s.sub = pubsub.NewSubscriber(s.queueSize, s.onOverflow)
		if s.onSub != nil {
			s.onSub(s.sub)
		}
	}
	s.out = s.out[:0]
	for _, ch := range args[1:] {
		if len(s.hub.Channels(s.sub)) >= maxSubscriptions {
			s.out = append(s.out, errTooManySubs)
			break
		}
		n := s.hub.Subscribe(s.sub, ch)
		s.out = append(s.out, resp.Array(resp.BulkString("subscribe"), resp.BulkString(ch), resp.Integer(int64(n))))
	}
	return s.out
}

// unsubscribe handles UNSUBSCRIBE [channel ...]. With no arguments it leaves
// every channel; with none subscribed it still answers once, with a null.
func (s *session) unsubscribe(args []string) []resp.Value {
	if s.hub == nil || s.sub == nil {
		return s.one(resp.Array(resp.BulkString("unsubscribe"), resp.Null(), resp.Integer(0)))
	}
	s.out = s.out[:0]
	names := args[1:]
	if len(names) == 0 {
		names = s.hub.Channels(s.sub)
		if len(names) == 0 {
			return s.one(resp.Array(resp.BulkString("unsubscribe"), resp.Null(), resp.Integer(0)))
		}
	}
	for _, ch := range names {
		n := s.hub.Unsubscribe(s.sub, ch)
		s.out = append(s.out, resp.Array(resp.BulkString("unsubscribe"), resp.BulkString(ch), resp.Integer(int64(n))))
	}
	return s.out
}
