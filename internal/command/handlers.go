package command

import (
	"fmt"
	"strings"

	"github.com/anmol0705/mini-redis-go/internal/resp"
)

var okReply = resp.SimpleString("OK")

func cmdPing(_ *Dispatcher, args []string) resp.Value {
	switch len(args) {
	case 1:
		return resp.SimpleString("PONG")
	case 2:
		return resp.BulkString(args[1])
	default:
		return wrongArgs("ping")
	}
}

func cmdEcho(_ *Dispatcher, args []string) resp.Value {
	return resp.BulkString(args[1])
}

// cmdSet accepts only "SET key value" for now. Options (EX, NX, ...) arrive
// in Phase 2; until then they are a syntax error rather than being ignored.
func cmdSet(d *Dispatcher, args []string) resp.Value {
	if len(args) != 3 {
		return syntaxError()
	}
	d.store.Set(args[1], args[2])
	return okReply
}

func cmdGet(d *Dispatcher, args []string) resp.Value {
	v, ok := d.store.Get(args[1])
	if !ok {
		return resp.Null()
	}
	return resp.BulkString(v)
}

func cmdDel(d *Dispatcher, args []string) resp.Value {
	var n int64
	for _, k := range args[1:] {
		if d.store.Delete(k) {
			n++
		}
	}
	return resp.Integer(n)
}

// cmdExists counts a key once per mention, as Redis does:
// EXISTS a a returns 2 when a exists.
func cmdExists(d *Dispatcher, args []string) resp.Value {
	var n int64
	for _, k := range args[1:] {
		if d.store.Exists(k) {
			n++
		}
	}
	return resp.Integer(n)
}

func cmdDBSize(d *Dispatcher, _ []string) resp.Value {
	return resp.Integer(int64(d.store.Len()))
}

// cmdFlush serves FLUSHALL and FLUSHDB. We have a single database, so they
// are the same. The optional ASYNC/SYNC word is accepted and ignored.
func cmdFlush(d *Dispatcher, args []string) resp.Value {
	if len(args) > 2 {
		return syntaxError()
	}
	if len(args) == 2 && !strings.EqualFold(args[1], "ASYNC") && !strings.EqualFold(args[1], "SYNC") {
		return syntaxError()
	}
	d.store.Clear()
	return okReply
}

// cmdCommand answers COMMAND (and COMMAND DOCS, which redis-cli sends on
// start-up) with an empty array: we do not describe our commands.
func cmdCommand(_ *Dispatcher, _ []string) resp.Value {
	return resp.Array()
}

func unknownSubcommand(args []string) resp.Value {
	return resp.Error(fmt.Sprintf("ERR unknown subcommand %s. Try %s HELP.",
		quote(args[1]), strings.ToUpper(args[0])))
}

// cmdClient accepts the two housekeeping subcommands client libraries send
// on connect. We do not store the values.
func cmdClient(_ *Dispatcher, args []string) resp.Value {
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
func cmdConfig(_ *Dispatcher, args []string) resp.Value {
	if strings.EqualFold(args[1], "GET") {
		if len(args) < 3 {
			return wrongArgs("config|get")
		}
		return resp.Array()
	}
	return unknownSubcommand(args)
}

// cmdInfo builds the INFO text. Sections are separated by blank lines and
// every line ends in CRLF, as in Redis.
func cmdInfo(d *Dispatcher, args []string) resp.Value {
	if len(args) > 2 {
		return syntaxError()
	}
	section := "default"
	if len(args) == 2 {
		section = strings.ToLower(args[1])
	}
	all := section == "default" || section == "all" || section == "everything"

	var info Info
	if d.info != nil {
		info = d.info.Info()
	}

	var parts []string
	if all || section == "server" {
		parts = append(parts, fmt.Sprintf("# Server\r\nredis_version:%s\r\ntcp_port:%d\r\nuptime_in_seconds:%d\r\n",
			info.Version, info.TCPPort, info.UptimeSeconds))
	}
	if all || section == "clients" {
		parts = append(parts, fmt.Sprintf("# Clients\r\nconnected_clients:%d\r\n", info.ConnectedClients))
	}
	if all || section == "keyspace" {
		ks := "# Keyspace\r\n"
		if n := d.store.Len(); n > 0 {
			// There are no expiries until Phase 2.
			ks += fmt.Sprintf("db0:keys=%d,expires=0,avg_ttl=0\r\n", n)
		}
		parts = append(parts, ks)
	}
	return resp.BulkString(strings.Join(parts, "\r\n"))
}
