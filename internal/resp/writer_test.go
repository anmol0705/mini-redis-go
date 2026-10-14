package resp

import (
	"bytes"
	"testing"
)

func TestWriter(t *testing.T) {
	// 1. Table-driven tests: We define a slice of structs, where each struct
	// holds the name of the test, the function to run, and the expected output.
	tests := []struct {
		name  string
		write func(w *Writer) error
		want  string
	}{
		{"simple string", func(w *Writer) error { return w.WriteSimpleString("OK") }, "+OK\r\n"},
		{"error", func(w *Writer) error { return w.WriteError("ERR unknown command") }, "-ERR unknown command\r\n"},
		{"integer", func(w *Writer) error { return w.WriteInteger(42) }, ":42\r\n"},
		{"bulk string Anmol", func(w *Writer) error { return w.WriteBulkString("Anmol") }, "$5\r\nAnmol\r\n"},
		{"empty bulk string", func(w *Writer) error { return w.WriteBulkString("") }, "$0\r\n\r\n"},
		{"null", func(w *Writer) error { return w.WriteNull() }, "$-1\r\n"},
		{"bulk string with unicode", func(w *Writer) error { return w.WriteBulkString("héllo") }, "$6\r\nhéllo\r\n"},
	}

	// 2. Loop over each test case and run it as a sub-test
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// bytes.Buffer acts as our "fake" network connection
			var buf bytes.Buffer
			w := NewWriter(&buf)

			// Execute the specific write function for this test case
			if err := tt.write(w); err != nil {
				t.Fatalf("write failed: %v", err)
			}

			// Flush the buffer to push data into buf
			if err := w.Flush(); err != nil {
				t.Fatalf("flush failed: %v", err)
			}

			// Compare the actual bytes written against our expected format
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
