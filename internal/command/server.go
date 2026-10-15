package command

import (
	"fmt"
	"strings"

	"github.com/anmol0705/mini-redis-go/internal/resp"
)

func cmdPing(x *Ctx) resp.Value {
	switch len(x.Args) {
	case 1:
		return resp.SimpleString("PONG")
	case 2:
		return resp.BulkString(x.Args[1])
	default:
		return wrongArgs("ping")
	}
}

func cmdEcho(x *Ctx) resp.Value { return resp.BulkString(x.Args[1]) }

// cmdCommand answers COMMAND (and COMMAND DOCS, which redis-cli sends on
// start-up) with an empty array: we do not describe our commands.
func cmdCommand(*Ctx) resp.Value { return resp.Array() }

// cmdClient accepts the two housekeeping subcommands client libraries send
// on connect. We do not store the values.
func cmdClient(x *Ctx) resp.Value {
	args := x.Args
	switch strings.ToUpper(args[1]) {
	case "SETNAME":
		if len(args) != 3 {
			return wrongArgs("client|setname")
		}
		return okReply
	case "SETINFO":
		if len(args) != 4 {
			return wrongArgs("client|setinfo")
		}
		return okReply
	default:
		return unknownSubcommand(args)
	}
}

// cmdConfig answers CONFIG GET with an empty array: we expose no settings.
func cmdConfig(x *Ctx) resp.Value {
	args := x.Args
	if strings.EqualFold(args[1], "GET") {
		if len(args) < 3 {
			return wrongArgs("config|get")
		}
		return resp.Array()
	}
	return unknownSubcommand(args)
}

func cmdPublish(x *Ctx) resp.Value {
	if x.Env == nil || x.Env.Publisher == nil {
		return resp.Integer(0)
	}
	return resp.Integer(int64(x.Env.Publisher.Publish(x.Args[1], x.Args[2])))
}

// cmdInfo builds the INFO text. Sections are separated by blank lines and
// every line ends in CRLF, as in Redis.
func cmdInfo(x *Ctx) resp.Value {
	args := x.Args
	if len(args) > 2 {
		return errSyntax
	}
	section := "default"
	if len(args) == 2 {
		section = strings.ToLower(args[1])
	}
	all := section == "default" || section == "all" || section == "everything"

	var env Env
	if x.Env != nil {
		env = *x.Env
	}
	var info Info
	if env.Info != nil {
		info = env.Info.Info()
	}
	st := x.DB.Stats()

	var parts []string
	if all || section == "server" {
		parts = append(parts, fmt.Sprintf("# Server\r\nredis_version:%s\r\ntcp_port:%d\r\nuptime_in_seconds:%d\r\n",
			info.Version, info.TCPPort, info.UptimeSeconds))
	}
	if all || section == "clients" {
		parts = append(parts, fmt.Sprintf("# Clients\r\nconnected_clients:%d\r\n", info.ConnectedClients))
	}
	if all || section == "memory" {
		parts = append(parts, fmt.Sprintf("# Memory\r\nused_memory:%d\r\nmaxmemory:%d\r\nmaxmemory_policy:%s\r\n",
			x.DB.UsedMemory(), env.MaxMemory, env.Policy))
	}
	if all || section == "stats" {
		parts = append(parts, fmt.Sprintf("# Stats\r\nexpired_keys:%d\r\nevicted_keys:%d\r\n", st.ExpiredKeys, st.EvictedKeys))
	}
	for _, name := range []string{"persistence", "replication"} {
		if !all && section != name {
			continue
		}
		text, ok := "", false
		if env.Sections != nil {
			text, ok = env.Sections.InfoSection(name)
		}
		if ok {
			parts = append(parts, text)
		}
	}
	if all || section == "keyspace" {
		ks := "# Keyspace\r\n"
		if n := x.DB.Len(); n > 0 {
			ks += fmt.Sprintf("db0:keys=%d,expires=%d,avg_ttl=0\r\n", n, x.DB.VolatileLen())
		}
		parts = append(parts, ks)
	}
	return resp.BulkString(strings.Join(parts, "\r\n"))
}

var errNoHooks = resp.Error("ERR this command is not available in this configuration")

func cmdSave(x *Ctx) resp.Value {
	if x.Env == nil || x.Env.Hooks == nil {
		return errNoHooks
	}
	if err := x.Env.Hooks.Save(x.DB); err != nil {
		return resp.Error("ERR " + err.Error())
	}
	return okReply
}

func cmdBGSave(x *Ctx) resp.Value {
	if x.Env == nil || x.Env.Hooks == nil {
		return errNoHooks
	}
	if len(x.Args) > 2 || (len(x.Args) == 2 && !strings.EqualFold(x.Args[1], "SCHEDULE")) {
		return errSyntax
	}
	status, err := x.Env.Hooks.BGSave(x.DB)
	if err != nil {
		return resp.Error("ERR " + err.Error())
	}
	return resp.SimpleString(status)
}

func cmdBGRewriteAOF(x *Ctx) resp.Value {
	if x.Env == nil || x.Env.Hooks == nil {
		return errNoHooks
	}
	status, err := x.Env.Hooks.RewriteAOF(x.DB)
	if err != nil {
		return resp.Error("ERR " + err.Error())
	}
	return resp.SimpleString(status)
}

// cmdReplicaOf implements REPLICAOF host port and REPLICAOF NO ONE.
func cmdReplicaOf(x *Ctx) resp.Value {
	if x.Env == nil || x.Env.Hooks == nil || x.Env.Control == nil {
		return errNoHooks
	}
	host, port := x.Args[1], x.Args[2]
	if strings.EqualFold(host, "NO") && strings.EqualFold(port, "ONE") {
		if err := x.Env.Hooks.ReplicaOfNoOne(); err != nil {
			return resp.Error("ERR " + err.Error())
		}
		x.Env.Control.SetReadOnly(false)
		return okReply
	}
	if n, err := parsePort(port); err != nil || n == 0 {
		return resp.Error("ERR Invalid master port")
	}
	if err := x.Env.Hooks.ReplicaOf(host, port); err != nil {
		return resp.Error("ERR " + err.Error())
	}
	x.Env.Control.SetReadOnly(true)
	return okReply
}

func parsePort(s string) (int, error) {
	n := 0
	if s == "" || len(s) > 5 {
		return 0, errNotInt0
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, errNotInt0
		}
		n = n*10 + int(c-'0')
	}
	if n > 65535 {
		return 0, errNotInt0
	}
	return n, nil
}

type constError string

func (e constError) Error() string { return string(e) }

const errNotInt0 = constError("not a port")
