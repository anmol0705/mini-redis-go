# 003 - Never trust a size from the network

Status: accepted (Phase 1)

## Context

RESP headers carry numbers: line length, array count, bulk length. A client can
announce huge values with a few bytes. The first reader allocated from those
numbers and buffered lines without limit.

## Decisions

| Threat | Fix | Where |
|---|---|---|
| Endless line with no `\n` | Read with `ReadSlice`, accumulate across `ErrBufferFull`, reject above 64 KiB (`ErrProtocol`). | `readLine` |
| Header `$536870912` then silence (512 MiB allocation for 10 bytes) | Values above 64 KiB are read in 64 KiB chunks into a `strings.Builder`; memory follows bytes received. Smaller values use one exact allocation. | `readLarge` |
| Header `*1048576` (16 MiB slice for 10 bytes) | Preallocate at most 64 slots, then `append`. | `ReadCommand` |
| Client text echoed into error replies (injection, huge replies) | `%q` escaping and a 64-byte cap per echoed item. | `command.quote` |
| CR/LF in a simple string or error | Writer replaces them with spaces. | `resp.sanitizeLine` |
| Too many connections | `-max-clients` (default 10000), extra clients get an error and are closed. | `Server.admit` |
| Silent connections hoarding slots | `-idle-timeout` (default off). | `connection.Read` |

`ReadSlice` returns a view into the reader's buffer that the next read
overwrites, so the reader copies it (`string(...)`) before reading again.
The 64 KiB line cap is checked on the *total* length, so a caller who builds a
`bufio.Reader` with a bigger buffer cannot bypass it.

## Consequences

- Maximum memory per connection from input is bounded by the data the client
  really sent, plus one 64 KiB chunk.
- A legitimately huge value still costs memory proportional to its size
  (Redis has the same 512 MiB limit). The limits are constants for now;
  making them flags is listed in known-limitations.
- Every row has a regression test (`reader_limits_test.go`, `value_test.go`,
  `command_test.go`).
