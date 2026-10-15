package resp

import (
	"bufio"
	"io"
	"strings"
	"testing"
)

// infiniteReader repeats data forever, so a benchmark can parse many commands
// without rebuilding its input.
type infiniteReader struct {
	data []byte
	pos  int
}

func (r *infiniteReader) Read(p []byte) (int, error) {
	n := copy(p, r.data[r.pos:])
	r.pos = (r.pos + n) % len(r.data)
	return n, nil
}

func benchRead(b *testing.B, cmd []string) {
	enc := AppendCommand(nil, cmd)
	r := bufio.NewReaderSize(&infiniteReader{data: enc}, 64*1024)
	b.SetBytes(int64(len(enc)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ReadCommand(r); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadCommandPing(b *testing.B) { benchRead(b, []string{"PING"}) }
func BenchmarkReadCommandSet(b *testing.B) {
	benchRead(b, []string{"SET", "key:000000012345", strings.Repeat("x", 32)})
}
func BenchmarkReadCommandSet1KiB(b *testing.B) {
	benchRead(b, []string{"SET", "key:000000012345", strings.Repeat("x", 1024)})
}
func BenchmarkReadCommandSet100KiB(b *testing.B) {
	benchRead(b, []string{"SET", "key:000000012345", strings.Repeat("x", 100<<10)})
}
func BenchmarkReadCommandMSet10(b *testing.B) {
	cmd := []string{"MSET"}
	for range 10 {
		cmd = append(cmd, "key:000000012345", strings.Repeat("x", 32))
	}
	benchRead(b, cmd)
}

func BenchmarkAppendCommand(b *testing.B) {
	cmd := []string{"SET", "key:000000012345", strings.Repeat("x", 32)}
	var buf []byte
	b.ReportAllocs()
	for b.Loop() {
		buf = AppendCommand(buf[:0], cmd)
	}
}

func BenchmarkWriteValues(b *testing.B) {
	w := NewWriter(io.Discard)
	vals := []Value{SimpleString("OK"), Integer(12345), BulkString(strings.Repeat("x", 32)), Null(),
		Array(BulkString("a"), BulkString("b"), Integer(1))}
	b.ReportAllocs()
	for b.Loop() {
		for _, v := range vals {
			if err := w.WriteValue(v); err != nil {
				b.Fatal(err)
			}
		}
	}
}
