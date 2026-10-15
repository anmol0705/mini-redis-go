package app

import (
	"context"
	"errors"
	"log/slog"

	"github.com/anmol0705/mini-redis-go/internal/engine"
	"github.com/anmol0705/mini-redis-go/internal/persist"
	"github.com/anmol0705/mini-redis-go/internal/repl"
	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// hooks adapts the node's components to command.Hooks and
// command.SectionSource. Its methods run on the engine goroutine.
type hooks struct {
	pers *persist.Manager
	repl *repl.Manager
}

func (h *hooks) Save(db *store.DB) error { return h.pers.Save(db) }

func (h *hooks) BGSave(db *store.DB) (string, error) { return h.pers.BGSave(db) }

func (h *hooks) RewriteAOF(db *store.DB) (string, error) { return h.pers.RewriteAOF(db) }

func (h *hooks) ReplicaOf(host, port string) error { return h.repl.ReplicaOf(host, port) }

func (h *hooks) ReplicaOfNoOne() error { return h.repl.ReplicaOfNoOne() }

// InfoSection serves the persistence and replication sections of INFO.
func (h *hooks) InfoSection(name string) (string, bool) {
	if s, ok := h.pers.InfoSection(name); ok {
		return s, true
	}
	return h.repl.InfoSection(name)
}

// multiPropagator feeds every change to several consumers in order.
type multiPropagator []engine.Propagator

func (m multiPropagator) Propagate(cmd []string) {
	for _, p := range m {
		p.Propagate(cmd)
	}
}

// Commit commits all consumers and returns the first error.
func (m multiPropagator) Commit() error {
	var first error
	for _, p := range m {
		if err := p.Commit(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// replApplier lets a repl.Replica change the node. Every method does its
// check-and-apply in ONE engine step, and refuses once ctx is cancelled, so a
// replica that was told REPLICAOF NO ONE can never apply a late command.
type replApplier struct {
	eng  *engine.Engine
	pers *persist.Manager
	log  *slog.Logger
}

func (a *replApplier) LoadSnapshot(ctx context.Context, items []store.Item) error {
	var err error
	execErr := a.eng.Exec(ctx, func(st *engine.State) {
		if ctx.Err() != nil {
			err = repl.ErrStopped
			return
		}
		if err = st.DB().Load(items); err != nil {
			return
		}
		err = a.pers.ResetAOF(items)
	})
	return errors.Join(execErr, err)
}

func (a *replApplier) Apply(ctx context.Context, args []string) error {
	var err error
	execErr := a.eng.Exec(ctx, func(st *engine.State) {
		if ctx.Err() != nil {
			err = repl.ErrStopped
			return
		}
		// A command that fails here means this replica has diverged from the
		// primary. Redis only logs it; so do we, and carry on.
		if r := st.ApplyFromMaster(args); r.Kind == resp.KindError {
			a.log.Warn("command from the primary failed on this replica", "command", args[0], "err", r.Str)
		}
	})
	return errors.Join(execErr, err)
}
