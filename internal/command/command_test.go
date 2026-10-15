package command

import (
	"reflect"
	"strings"
	"testing"

	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

type fakeInfo Info

func (f fakeInfo) Info() Info { return Info(f) }

func newTestDispatcher() *Dispatcher {
	return New(store.New(), fakeInfo{Version: "9.9.9", TCPPort: 6380, UptimeSeconds: 42, ConnectedClients: 3})
}

// run is a shorthand: run(d, "SET", "k", "v").
func run(d *Dispatcher, args ...string) resp.Value { return d.Execute(args) }

func TestExecute(t *testing.T) {
	tests := []struct {
		name string
		cmds [][]string // all but the last are setup; the last one is checked
		want resp.Value
	}{
		{"ping", [][]string{{"PING"}}, resp.SimpleString("PONG")},
		{"ping with message", [][]string{{"PING", "hi"}}, resp.BulkString("hi")},
		{"ping too many", [][]string{{"PING", "a", "b"}}, resp.Error("ERR wrong number of arguments for 'ping' command")},
		{"echo", [][]string{{"ECHO", "hey"}}, resp.BulkString("hey")},
		{"echo no arg", [][]string{{"ECHO"}}, resp.Error("ERR wrong number of arguments for 'echo' command")},
		{"lower case name", [][]string{{"ping"}}, resp.SimpleString("PONG")},
		{"mixed case name", [][]string{{"PiNg"}}, resp.SimpleString("PONG")},

		{"set", [][]string{{"SET", "k", "v"}}, resp.SimpleString("OK")},
		{"set missing value", [][]string{{"SET", "k"}}, resp.Error("ERR wrong number of arguments for 'set' command")},
		{"set with option is not yet supported", [][]string{{"SET", "k", "v", "EX", "10"}}, resp.Error("ERR syntax error")},
		{"get existing", [][]string{{"SET", "k", "v"}, {"GET", "k"}}, resp.BulkString("v")},
		{"get empty value", [][]string{{"SET", "k", ""}, {"GET", "k"}}, resp.BulkString("")},
		{"get missing", [][]string{{"GET", "k"}}, resp.Null()},
		{"get wrong arity", [][]string{{"GET"}}, resp.Error("ERR wrong number of arguments for 'get' command")},

		{"del counts removed keys", [][]string{{"SET", "a", "1"}, {"SET", "b", "2"}, {"DEL", "a", "b", "c"}}, resp.Integer(2)},
		{"del then get", [][]string{{"SET", "a", "1"}, {"DEL", "a"}, {"GET", "a"}}, resp.Null()},
		{"del no keys", [][]string{{"DEL"}}, resp.Error("ERR wrong number of arguments for 'del' command")},
		{"exists counts each mention", [][]string{{"SET", "a", "1"}, {"EXISTS", "a", "a", "b"}}, resp.Integer(2)},

		{"dbsize empty", [][]string{{"DBSIZE"}}, resp.Integer(0)},
		{"dbsize", [][]string{{"SET", "a", "1"}, {"SET", "b", "1"}, {"DBSIZE"}}, resp.Integer(2)},
		{"dbsize extra arg", [][]string{{"DBSIZE", "x"}}, resp.Error("ERR wrong number of arguments for 'dbsize' command")},
		{"flushall", [][]string{{"SET", "a", "1"}, {"FLUSHALL"}}, resp.SimpleString("OK")},
		{"flushall empties", [][]string{{"SET", "a", "1"}, {"FLUSHALL"}, {"DBSIZE"}}, resp.Integer(0)},
		{"flushdb empties", [][]string{{"SET", "a", "1"}, {"FLUSHDB", "async"}, {"DBSIZE"}}, resp.Integer(0)},
		{"flushall bad option", [][]string{{"FLUSHALL", "now"}}, resp.Error("ERR syntax error")},

		{"command", [][]string{{"COMMAND"}}, resp.Array()},
		{"command docs", [][]string{{"COMMAND", "DOCS"}}, resp.Array()},
		{"client setinfo", [][]string{{"CLIENT", "SETINFO", "LIB-NAME", "x"}}, resp.SimpleString("OK")},
		{"client setname", [][]string{{"CLIENT", "SETNAME", "me"}}, resp.SimpleString("OK")},
		{"client setname arity", [][]string{{"CLIENT", "SETNAME"}}, resp.Error("ERR wrong number of arguments for 'client|setname' command")},
		{"client unknown", [][]string{{"CLIENT", "NOPE"}}, resp.Error(`ERR unknown subcommand "NOPE". Try CLIENT HELP.`)},
		{"config get", [][]string{{"CONFIG", "GET", "save"}}, resp.Array()},
		{"config get without pattern", [][]string{{"CONFIG", "GET"}}, resp.Error("ERR wrong number of arguments for 'config|get' command")},
		{"config set unsupported", [][]string{{"CONFIG", "SET", "a", "b"}}, resp.Error(`ERR unknown subcommand "SET". Try CONFIG HELP.`)},

		{"unknown command", [][]string{{"NOSUCH", "a", "b"}}, resp.Error(`ERR unknown command "NOSUCH", with args beginning with: "a" "b"`)},
		{"empty command", [][]string{{}}, resp.Error("ERR empty command")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDispatcher()
			var got resp.Value
			for _, c := range tt.cmds {
				got = d.Execute(c)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("got %+v; want %+v", got, tt.want)
			}
		})
	}
}

// Client text must never reach the wire raw: a command name containing
// CRLF must not be able to start a new protocol line.
func TestUnknownCommandEscapesClientText(t *testing.T) {
	got := run(newTestDispatcher(), "BAD\r\n+OK", "arg\r\n:1")
	if got.Kind != resp.KindError {
		t.Fatalf("kind = %v; want error", got.Kind)
	}
	if strings.ContainsAny(got.Str, "\r\n") {
		t.Errorf("reply contains raw CR/LF: %q", got.Str)
	}
	if !strings.Contains(got.Str, `\r\n`) {
		t.Errorf("reply %q does not show the escaped bytes", got.Str)
	}
}

func TestUnknownCommandTruncatesLongText(t *testing.T) {
	got := run(newTestDispatcher(), strings.Repeat("A", 10000), "x", "y", "z", "w")
	if len(got.Str) > 500 {
		t.Errorf("error reply is %d bytes; want it capped", len(got.Str))
	}
	if strings.Contains(got.Str, `"w"`) {
		t.Errorf("echoed more than three arguments: %q", got.Str)
	}
}

func TestInfo(t *testing.T) {
	d := newTestDispatcher()

	all := run(d, "INFO")
	if all.Kind != resp.KindBulkString {
		t.Fatalf("kind = %v; want bulk string", all.Kind)
	}
	for _, want := range []string{
		"# Server\r\n", "redis_version:9.9.9\r\n", "tcp_port:6380\r\n", "uptime_in_seconds:42\r\n",
		"# Clients\r\n", "connected_clients:3\r\n", "# Keyspace\r\n",
	} {
		if !strings.Contains(all.Str, want) {
			t.Errorf("INFO is missing %q; got %q", want, all.Str)
		}
	}
	if strings.Contains(all.Str, "db0:") {
		t.Errorf("empty keyspace must not list db0: %q", all.Str)
	}

	run(d, "SET", "a", "1")
	if ks := run(d, "INFO", "keyspace").Str; ks != "# Keyspace\r\ndb0:keys=1,expires=0,avg_ttl=0\r\n" {
		t.Errorf("INFO keyspace = %q", ks)
	}
	if got := run(d, "INFO", "CLIENTS").Str; got != "# Clients\r\nconnected_clients:3\r\n" {
		t.Errorf("INFO CLIENTS = %q", got)
	}
	if got := run(d, "INFO", "nosuchsection").Str; got != "" {
		t.Errorf("unknown section = %q; want empty", got)
	}
	if got := run(d, "INFO", "a", "b"); got.Str != "ERR syntax error" {
		t.Errorf("INFO a b = %+v", got)
	}
}

func TestInfoWithoutSource(t *testing.T) {
	d := New(store.New(), nil)
	if got := run(d, "INFO", "clients").Str; got != "# Clients\r\nconnected_clients:0\r\n" {
		t.Errorf("got %q", got)
	}
}
