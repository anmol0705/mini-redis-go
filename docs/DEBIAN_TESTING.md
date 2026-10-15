# Testing on Debian

Step-by-step commands for the Linux machine. Everything runs as a normal user
except the `apt` lines. Tested path: Debian 12 or newer; any recent Linux works.

## 0. What to expect

The Go test suite already passes on Windows. This guide checks the parts that
need Linux tools: `redis-cli`, `redis-benchmark`, `kill -9`, real signals, and two
servers talking over the network.

## 1. Install the tools

```bash
sudo apt update
sudo apt install -y git build-essential redis-tools
go version      # needs Go 1.25 or newer
```

If `go version` is missing or older than 1.25, install the official tarball
(adjust the version to the newest 1.25.x or later):

```bash
curl -LO https://go.dev/dl/go1.25.3.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.25.3.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin     # also add this line to ~/.profile
go version
```

`redis-tools` provides `redis-cli` and `redis-benchmark`. `build-essential`
provides `gcc`, needed by `go test -race`.

Optional, for comparison benchmarks (it does not need to be running):

```bash
sudo apt install -y redis-server
sudo systemctl disable --now redis-server    # we start our own copy on other ports
```

## 2. Get the code and run all checks

```bash
git clone <your repo url> mini-redis-go && cd mini-redis-go
git checkout v1.0

go build ./...
go vet ./...
test -z "$(gofmt -l .)" && echo "gofmt ok"
go test -race -count=1 -cover ./...
GOOS=windows go vet ./...      # the cross-platform check
```

Expected: no output from `vet` and `gofmt -l`, every package `ok`.

## 3. Build and run

```bash
mkdir -p bin && go build -o bin/miniredis ./cmd/miniredis
./bin/miniredis -addr 127.0.0.1:6379 -log-level info
```

Useful flags (all optional): `-max-clients`, `-idle-timeout`,
`-maxmemory 100mb -maxmemory-policy allkeys-lru`, `-dir`, `-appendonly yes`,
`-appendfsync always|everysec|no`, `-dbfilename`, `-snapshot-interval 5m`,
`-replicaof "host port"`, `-pprof-addr 127.0.0.1:6060`. `./bin/miniredis -h` lists them.

## 4. Smoke test with redis-cli (second terminal)

```bash
redis-cli -p 6379 PING                      # PONG
redis-cli -p 6379 SET name Anmol            # OK
redis-cli -p 6379 GET name                  # "Anmol"
redis-cli -p 6379 RPUSH list a b c          # (integer) 3
redis-cli -p 6379 LRANGE list 0 -1
redis-cli -p 6379 ZADD board 5 alice 3 bob
redis-cli -p 6379 ZRANGE board 0 -1 WITHSCORES
redis-cli -p 6379 SET temp x EX 5 && redis-cli -p 6379 TTL temp
redis-cli -p 6379 INFO                      # server, clients, memory, persistence, replication, keyspace
redis-cli -p 6379                           # interactive mode: type commands, try MULTI / EXEC
```

Note: inline commands are not supported (`telnet` + `PING` gets a protocol error).
That is deliberate and listed in `docs/known-limitations.md`.

## 5. Conformance suite

With the server running on 6379 (it empties the database, so use a scratch server):

```bash
conformance/run.sh 127.0.0.1 6379
```

All checks should print `ok`. A `note` about `redis-cli --pipe` is expected (it
needs inline commands). If a case fails only because of output formatting, the
Debian `redis-cli` differs from the one the files were verified with; run the
same script against `redis-server` (`redis-server --port 6380 &`) to see what the
reference does, and open an issue in your notes. To regenerate expectations from
a real Redis: `conformance/run.sh 127.0.0.1 6380 --record`, then `git diff`.

## 6. Benchmarks with redis-benchmark

Plain run (the default test list starts with `PING_INLINE`, which fails here, so
select tests with `-t`):

```bash
redis-benchmark -p 6379 -t set,get -n 100000
redis-benchmark -p 6379 -t set,get,incr,lpush,rpush,lpop,rpop,sadd,hset -n 100000 -P 16
```

If `redis-benchmark` refuses to start, report the exact message: it sends
`CONFIG GET` at startup and our reply is an empty array (see the known
limitations). The fallback would be to reply with pairs for `save` and
`appendonly`.

Automated comparison with real Redis and saved results:

```bash
bench/run.sh -n 100000 -c 50      # writes bench/results/<timestamp>/summary.md
```

Fill the "TBD" cells in `README.md` from that summary. Go micro-benchmarks:

```bash
make bench
```

## 7. Crash test (kill -9)

```bash
scripts/crash-test.sh 1000        # N keys, default port 6390
```

It starts the server with `-appendonly yes -appendfsync always`, writes N keys,
`kill -9`s it, restarts and verifies every key; then appends half a command to
the file, restarts, and checks the repair was logged. Expected last line: `PASS`.

## 8. Persistence by hand

```bash
mkdir -p /tmp/mr && ./bin/miniredis -addr 127.0.0.1:6379 -dir /tmp/mr -appendonly yes &
redis-cli SET a 1 && redis-cli RPUSH l x y && redis-cli SET t v EX 100
redis-cli BGREWRITEAOF && redis-cli INFO persistence
redis-cli SAVE && ls -l /tmp/mr          # appendonly.aof and dump.mrdb
kill -TERM %1; wait                       # graceful stop, exit status 0
./bin/miniredis -addr 127.0.0.1:6379 -dir /tmp/mr -appendonly yes &
redis-cli GET a; redis-cli LRANGE l 0 -1; redis-cli TTL t
kill -TERM %1; wait
```

## 9. Two-instance replication (ports 6379 and 6380)

```bash
./bin/miniredis -addr 127.0.0.1:6379 &                      # primary
./bin/miniredis -addr 127.0.0.1:6380 &                      # replica
redis-cli -p 6379 SET k v && redis-cli -p 6379 SADD s a b
redis-cli -p 6380 REPLICAOF 127.0.0.1 6379                  # or start with -replicaof "127.0.0.1 6379"
sleep 1
redis-cli -p 6380 GET k                                     # "v"
redis-cli -p 6380 SET x y                                   # READONLY error
redis-cli -p 6379 INFO replication                          # connected_slaves:1, sync_full:1
redis-cli -p 6380 INFO replication                          # role:slave, master_link_status:up

# live stream and expiry
redis-cli -p 6379 SET short v PX 300; sleep 1; redis-cli -p 6380 EXISTS short   # 0

# promote the replica to a primary again
redis-cli -p 6380 REPLICAOF NO ONE
redis-cli -p 6380 SET x y                                   # now OK
```

There is no command to cut the link by hand (`CLIENT KILL` is not implemented),
and restarting the replica process loses its offset, which correctly forces a full
sync. Partial resync is demonstrated by the Go tests, which cut the connection
themselves: `go test -v -run 'PartialResync|BacklogTooSmall' ./internal/app`
(look for `sync_partial_ok` and `sync_full` in `INFO replication`).

Stop everything: `kill %1 %2` (or `pkill miniredis`).

> This replication protocol is private to miniredis. A miniredis replica cannot
> follow a real Redis, and a real Redis replica cannot follow miniredis.

## 10. Pub/Sub

Terminal 1: `redis-cli -p 6379 SUBSCRIBE news`
Terminal 2: `redis-cli -p 6379 PUBLISH news hello` (prints `1`; terminal 1 shows the message)

## 11. Graceful shutdown with a client connected

```bash
./bin/miniredis -addr 127.0.0.1:6379 & PID=$!
redis-cli -p 6379 SUBSCRIBE news &       # a connected client
sleep 1; kill -TERM $PID; wait $PID; echo "exit status: $?"    # expect 0
```

## 12. Fuzzing and profiling (optional)

```bash
make fuzz-short                  # 10 seconds on each of the five fuzz targets
./bin/miniredis -addr 127.0.0.1:6379 -pprof-addr 127.0.0.1:6060 &
go tool pprof -top http://127.0.0.1:6060/debug/pprof/profile?seconds=5
```

## What to send back

- the output of step 2 (any failure at all),
- the failing lines (if any) from steps 5 and 7,
- `bench/results/*/summary.md` from step 6,
- anything surprising in steps 4, 8, 9.
