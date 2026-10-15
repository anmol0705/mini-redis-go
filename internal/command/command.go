// Package command turns a parsed client command (a slice of strings) into a
// reply by running it against a store.DB.
//
// Concurrency: nothing here is synchronised. Commands run on the engine's
// single goroutine, which owns the DB; the command table is built once and
// only read afterwards. Commands that belong to one connection rather than
// to the data (QUIT, MULTI/EXEC, SUBSCRIBE) live in the server's session; see
// docs/decisions/002-session-layer.md.
//
// Propagation: besides a reply, a write command can say what should be
// logged (AOF) and sent to replicas. By default that is the command itself;
// commands whose effect depends on the clock, such as EXPIRE or SET ... EX,
// rewrite themselves into an absolute form (PEXPIREAT, SET ... PXAT) so that
// replaying the log later gives the same result.
package command

import (
	"fmt"
	"strings"

	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// Info is the runtime data INFO reports about the server process. It belongs
// to the server (uptime, client count, listening port), so the command
// package only describes the shape it needs.
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

// Publisher delivers a message to subscribers of a channel and returns how
// many received it.
type Publisher interface {
	Publish(channel, message string) int
}

// SectionSource supplies extra INFO sections (persistence, replication)
// owned by other components. ok is false for a section it does not know.
type SectionSource interface {
	InfoSection(name string) (text string, ok bool)
}

// Env is everything a command may reach besides the DB. Every field is
// optional.
type Env struct {
	Info      InfoSource
	Publisher Publisher
	Sections  SectionSource
	Hooks     Hooks
	Control   Control
	// MaxMemory and Policy are reported by INFO; the engine enforces them.
	MaxMemory int64
	Policy    store.Policy
}

// Ctx is the state of one command execution. The engine keeps a single Ctx
// and calls Reset before each command.
type Ctx struct {
	DB   *store.DB
	Args []string
	Env  *Env

	effects  [][]string
	rewrite  bool
	noEffect bool
}

// Reset prepares x to run args.
func (x *Ctx) Reset(args []string) {
	x.Args = args
	x.effects = x.effects[:0]
	x.rewrite, x.noEffect = false, false
}

// emit replaces the default propagation (the command itself) with cmd. It may
// be called several times to propagate several commands.
func (x *Ctx) emit(cmd ...string) {
	x.rewrite = true
	x.effects = append(x.effects, cmd)
}

// skipPropagation says this execution changed nothing and need not be logged.
func (x *Ctx) skipPropagation() { x.noEffect = true }

// Propagation returns the commands to log and replicate after s ran with the
// given reply: the command itself by default, nothing for read commands or
// errors, or whatever the handler asked for. The result is valid until the
// next Reset.
func (x *Ctx) Propagation(s *Spec, reply resp.Value) [][]string {
	if !s.Write || x.noEffect || reply.Kind == resp.KindError {
		return nil
	}
	if x.rewrite {
		return x.effects
	}
	x.effects = append(x.effects[:0], x.Args)
	return x.effects
}

// Spec describes one command.
type Spec struct {
	// Name is lower-case, used in error messages.
	Name string
	// Arity follows Redis: positive means exactly that many words including
	// the name; negative means at least that many.
	Arity int
	// Write marks commands that change data: they are logged, replicated
	// and refused on read-only replicas.
	Write bool
	// DenyOOM marks writes that can grow memory: they are refused when
	// maxmemory is reached and nothing can be evicted.
	DenyOOM bool

	run func(x *Ctx) resp.Value
}

// Run executes the command. The caller has already resolved it with Resolve,
// so the argument count is valid.
func (s *Spec) Run(x *Ctx) resp.Value { return s.run(x) }

// maxEchoLen limits how much client text we copy into an error message.
const maxEchoLen = 64

// Resolve finds the command named by args[0] and checks the argument count.
// On failure it returns a nil Spec and the error reply to send.
func Resolve(args []string) (*Spec, resp.Value) {
	if len(args) == 0 {
		return nil, resp.Error("ERR empty command")
	}
	s, ok := table[args[0]]
	if !ok {
		s, ok = table[strings.ToUpper(args[0])]
	}
	if !ok {
		return nil, resp.Error(unknownCommand(args))
	}
	if (s.Arity > 0 && len(args) != s.Arity) || (s.Arity < 0 && len(args) < -s.Arity) {
		return nil, wrongArgs(s.Name)
	}
	return s, resp.Value{}
}

// Execute resolves and runs one command. It is a convenience for callers that
// do not need to look at the Spec; the engine uses Resolve and Run so it can
// apply policy in between.
func Execute(x *Ctx, args []string) resp.Value {
	s, errReply := Resolve(args)
	if s == nil {
		return errReply
	}
	x.Reset(args)
	return s.Run(x)
}

func wrongArgs(name string) resp.Value {
	return resp.Error(fmt.Sprintf("ERR wrong number of arguments for '%s' command", name))
}

var (
	errSyntax = resp.Error("ERR syntax error")
	errNotInt = resp.Error(store.ErrNotInteger.Error())
	errFloat  = resp.Error("ERR value is not a valid float")
	okReply   = resp.SimpleString("OK")
)

// errReply converts a store error, whose text is already a Redis error
// message, into a reply.
func errReply(err error) resp.Value { return resp.Error(err.Error()) }

// quote escapes client text for use inside an error message. %q turns CR, LF
// and other control bytes into visible escapes, so a client cannot craft a
// command name that forges extra protocol lines; the cut keeps a huge
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

func unknownSubcommand(args []string) resp.Value {
	return resp.Error(fmt.Sprintf("ERR unknown subcommand %s. Try %s HELP.",
		quote(args[1]), strings.ToUpper(args[0])))
}

// table maps upper-case command names to their spec. It is built once and
// never modified, so reading it needs no lock.
var table = buildTable()

func buildTable() map[string]*Spec {
	specs := []*Spec{
		// connection and server
		{Name: "ping", Arity: -1, run: cmdPing},
		{Name: "echo", Arity: 2, run: cmdEcho},
		{Name: "command", Arity: -1, run: cmdCommand},
		{Name: "client", Arity: -2, run: cmdClient},
		{Name: "config", Arity: -2, run: cmdConfig},
		{Name: "info", Arity: -1, run: cmdInfo},
		{Name: "publish", Arity: 3, run: cmdPublish},
		{Name: "save", Arity: 1, run: cmdSave},
		{Name: "bgsave", Arity: -1, run: cmdBGSave},
		{Name: "bgrewriteaof", Arity: 1, run: cmdBGRewriteAOF},
		{Name: "replicaof", Arity: 3, run: cmdReplicaOf},

		// strings
		{Name: "set", Arity: -3, Write: true, DenyOOM: true, run: cmdSet},
		{Name: "get", Arity: 2, run: cmdGet},
		{Name: "mget", Arity: -2, run: cmdMGet},
		{Name: "mset", Arity: -3, Write: true, DenyOOM: true, run: cmdMSet},
		{Name: "incr", Arity: 2, Write: true, DenyOOM: true, run: cmdIncr},
		{Name: "decr", Arity: 2, Write: true, DenyOOM: true, run: cmdDecr},
		{Name: "incrby", Arity: 3, Write: true, DenyOOM: true, run: cmdIncrBy},
		{Name: "decrby", Arity: 3, Write: true, DenyOOM: true, run: cmdDecrBy},
		{Name: "append", Arity: 3, Write: true, DenyOOM: true, run: cmdAppend},
		{Name: "strlen", Arity: 2, run: cmdStrLen},

		// keys
		{Name: "del", Arity: -2, Write: true, run: cmdDel},
		{Name: "exists", Arity: -2, run: cmdExists},
		{Name: "expire", Arity: 3, Write: true, run: cmdExpire},
		{Name: "pexpire", Arity: 3, Write: true, run: cmdPExpire},
		{Name: "expireat", Arity: 3, Write: true, run: cmdExpireAt},
		{Name: "pexpireat", Arity: 3, Write: true, run: cmdPExpireAt},
		{Name: "ttl", Arity: 2, run: cmdTTL},
		{Name: "pttl", Arity: 2, run: cmdPTTL},
		{Name: "persist", Arity: 2, Write: true, run: cmdPersist},
		{Name: "type", Arity: 2, run: cmdType},
		{Name: "keys", Arity: 2, run: cmdKeys},
		{Name: "dbsize", Arity: 1, run: cmdDBSize},
		{Name: "flushall", Arity: -1, Write: true, run: cmdFlush},
		{Name: "flushdb", Arity: -1, Write: true, run: cmdFlush},

		// lists
		{Name: "lpush", Arity: -3, Write: true, DenyOOM: true, run: cmdLPush},
		{Name: "rpush", Arity: -3, Write: true, DenyOOM: true, run: cmdRPush},
		{Name: "lpop", Arity: -2, Write: true, run: cmdLPop},
		{Name: "rpop", Arity: -2, Write: true, run: cmdRPop},
		{Name: "llen", Arity: 2, run: cmdLLen},
		{Name: "lrange", Arity: 4, run: cmdLRange},
		{Name: "lindex", Arity: 3, run: cmdLIndex},

		// hashes
		{Name: "hset", Arity: -4, Write: true, DenyOOM: true, run: cmdHSet},
		{Name: "hget", Arity: 3, run: cmdHGet},
		{Name: "hdel", Arity: -3, Write: true, run: cmdHDel},
		{Name: "hgetall", Arity: 2, run: cmdHGetAll},
		{Name: "hexists", Arity: 3, run: cmdHExists},
		{Name: "hlen", Arity: 2, run: cmdHLen},

		// sets
		{Name: "sadd", Arity: -3, Write: true, DenyOOM: true, run: cmdSAdd},
		{Name: "srem", Arity: -3, Write: true, run: cmdSRem},
		{Name: "sismember", Arity: 3, run: cmdSIsMember},
		{Name: "smembers", Arity: 2, run: cmdSMembers},
		{Name: "scard", Arity: 2, run: cmdSCard},

		// sorted sets
		{Name: "zadd", Arity: -4, Write: true, DenyOOM: true, run: cmdZAdd},
		{Name: "zrem", Arity: -3, Write: true, run: cmdZRem},
		{Name: "zscore", Arity: 3, run: cmdZScore},
		{Name: "zrank", Arity: 3, run: cmdZRank},
		{Name: "zrange", Arity: -4, run: cmdZRange},
		{Name: "zcard", Arity: 2, run: cmdZCard},
	}
	m := make(map[string]*Spec, len(specs))
	for _, s := range specs {
		m[strings.ToUpper(s.Name)] = s
	}
	return m
}

// Hooks connects commands to components that live outside the keyspace:
// persistence and replication. They are called on the engine goroutine, so
// they must return quickly and hand long work to their own goroutines.
// A nil Hooks makes the corresponding commands reply with an error.
type Hooks interface {
	// Save writes a snapshot synchronously (SAVE).
	Save(db *store.DB) error
	// BGSave copies the keyspace and writes the snapshot in the background.
	BGSave(db *store.DB) (status string, err error)
	// RewriteAOF starts a background compaction of the append-only file.
	RewriteAOF(db *store.DB) (status string, err error)
	// ReplicaOf makes this server a replica of host:port.
	ReplicaOf(host, port string) error
	// ReplicaOfNoOne turns a replica back into a primary.
	ReplicaOfNoOne() error
}

// Control lets a command change engine state that is not part of the DB.
type Control interface {
	// SetReadOnly makes the engine refuse writes from ordinary clients
	// (replica mode) or accept them again.
	SetReadOnly(on bool)
}
