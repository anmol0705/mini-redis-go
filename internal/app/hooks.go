package app

import (
	"errors"

	"github.com/anmol0705/mini-redis-go/internal/persist"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// hooks adapts the node's components to command.Hooks and
// command.SectionSource. Its methods run on the engine goroutine.
type hooks struct {
	pers *persist.Manager
}

func (h *hooks) Save(db *store.DB) error { return h.pers.Save(db) }

func (h *hooks) BGSave(db *store.DB) (string, error) { return h.pers.BGSave(db) }

func (h *hooks) RewriteAOF(db *store.DB) (string, error) { return h.pers.RewriteAOF(db) }

func (h *hooks) ReplicaOf(host, port string) error {
	return errors.New("replication is not available")
}

func (h *hooks) ReplicaOfNoOne() error { return errors.New("replication is not available") }

// InfoSection serves the persistence and replication sections of INFO.
func (h *hooks) InfoSection(name string) (string, bool) {
	if s, ok := h.pers.InfoSection(name); ok {
		return s, true
	}
	if name == "replication" {
		return "# Replication\r\nrole:master\r\nconnected_slaves:0\r\nmaster_repl_offset:0\r\n", true
	}
	return "", false
}
