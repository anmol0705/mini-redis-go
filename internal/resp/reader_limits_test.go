package resp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
)

// countingReader yields an endless stream of one byte and counts how much
// of it the reader consumed.
type countingReader struct {
	b    byte
	read int
}

func (c *countingReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = c.b
	}
	c.read += len(p)
	return len(p), nil
}

// A client that never sends "\n" must not make us buffer without limit.
func TestReadCommandLineTooLong(t *testing.T) {
	src := &countingReader{b: 'a'}
	_, err := ReadCommand(bufio.NewReader(src))
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("err = %v; want ErrProtocol", err)
	}
	// We may read a little past the cap (one bufio fill), never megabytes.
	if limit := maxLineLen + 2*4096; src.read > limit {
		t.Errorf("consumed %d bytes of an endless line; want at most %d", src.read, limit)
	}
}

func TestReadCommandLongLineWithinCap(t *testing.T) {
	// A header whose digits span several bufio buffers but stays under
	// the cap is still a (bad) header, reported as such and not as "too long".
	input := "*" + strings.Repeat("1", 10000) + "\r\n"
	_, err := ReadCommand(bufio.NewReader(strings.NewReader(input)))
	if !errors.Is(err, ErrProtocol) || strings.Contains(err.Error(), "too long") {
		t.Fatalf("err = %v; want invalid array length", err)
	}
}

func TestReadCommandLineTooLongWithBigBuffer(t *testing.T) {
	// A caller-supplied buffer larger than the cap must not bypass it.
	input := "*" + strings.Repeat("1", maxLineLen) + "\r\n"
	r := bufio.NewReaderSize(strings.NewReader(input), 4*maxLineLen)
	if _, err := ReadCommand(r); !errors.Is(err, ErrProtocol) {
		t.Fatalf("err = %v; want ErrProtocol", err)
	}
}

// A bulk header may announce 512 MiB; memory must follow the bytes that
// actually arrive.
func TestReadCommandBulkDoesNotPreallocate(t *testing.T) {
	input := "*1\r\n$536870912\r\nonly a few bytes"

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := ReadCommand(bufio.NewReader(strings.NewReader(input)))
	runtime.ReadMemStats(&after)

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v; want io.ErrUnexpectedEOF", err)
	}
	const bound = 4 << 20
	if grew := after.TotalAlloc - before.TotalAlloc; grew > bound {
		t.Errorf("allocated %d bytes for a %d byte message; want < %d", grew, len(input), bound)
	}
}

func TestReadCommandArrayDoesNotPreallocate(t *testing.T) {
	input := "*1048576\r\n"

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := ReadCommand(bufio.NewReader(strings.NewReader(input)))
	runtime.ReadMemStats(&after)

	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v; want io.ErrUnexpectedEOF", err)
	}
	const bound = 1 << 20 // a full preallocation would be 16 MiB
	if grew := after.TotalAlloc - before.TotalAlloc; grew > bound {
		t.Errorf("allocated %d bytes for a 10 byte message; want < %d", grew, bound)
	}
}

func TestReadCommandBulkTooBig(t *testing.T) {
	input := "*1\r\n$536870913\r\n"
	_, err := ReadCommand(bufio.NewReader(strings.NewReader(input)))
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("err = %v; want ErrProtocol", err)
	}
}

func TestReadCommandLargeValueRoundTrip(t *testing.T) {
	// Larger than readChunk, and not a multiple of it, to exercise the
	// chunked path including the last partial chunk.
	value := strings.Repeat("x", 3*readChunk+17)
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteArrayHeader(2); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"SET", value} {
		if err := w.WriteBulkString(s); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}

	got, err := ReadCommand(bufio.NewReader(&buf))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[1] != value {
		t.Errorf("large value did not survive the round trip (got %d args)", len(got))
	}
}

func TestReadCommandLargeValueBadTerminator(t *testing.T) {
	value := strings.Repeat("x", readChunk+1)
	input := "*1\r\n$" + "65537" + "\r\n" + value + "ZZ"
	_, err := ReadCommand(bufio.NewReader(strings.NewReader(input)))
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("err = %v; want ErrProtocol", err)
	}
}

// A header split across bufio buffer boundaries must be reassembled.
func TestReadCommandHeaderAcrossSmallBuffer(t *testing.T) {
	input := "*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n"
	r := bufio.NewReaderSize(strings.NewReader(input), 16) // 16 is bufio's minimum
	got, err := ReadCommand(r)
	if err != nil || len(got) != 2 || got[1] != "hello" {
		t.Fatalf("got %q, %v", got, err)
	}
}
