package resp

import (
	"bytes"
	"testing"
)

func TestWriteValue(t *testing.T) {
	tests := []struct {
		name string
		v    Value
		want string
	}{
		{"simple string", SimpleString("OK"), "+OK\r\n"},
		{"error", Error("ERR boom"), "-ERR boom\r\n"},
		{"integer", Integer(-7), ":-7\r\n"},
		{"bulk string", BulkString("hi"), "$2\r\nhi\r\n"},
		{"null", Null(), "$-1\r\n"},
		{"empty array", Array(), "*0\r\n"},
		{"array", Array(BulkString("a"), Integer(1)), "*2\r\n$1\r\na\r\n:1\r\n"},
		{"nested array", Array(Array(), Array(SimpleString("x"))), "*2\r\n*0\r\n*1\r\n+x\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			if err := w.WriteValue(tt.v); err != nil {
				t.Fatal(err)
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			if got := buf.String(); got != tt.want {
				t.Errorf("got %q; want %q", got, tt.want)
			}
		})
	}
}

func TestWriteValueZeroKindIsAnError(t *testing.T) {
	w := NewWriter(&bytes.Buffer{})
	if err := w.WriteValue(Value{}); err == nil {
		t.Error("writing the zero Value succeeded; want an error")
	}
}

// Regression: CR/LF inside a status or error line would end it early and
// let the rest be read as a second reply.
func TestWriterStripsLineBreaksFromLines(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	if err := w.WriteError("ERR bad\r\n+INJECTED"); err != nil {
		t.Fatal(err)
	}
	if err := w.WriteSimpleString("a\nb"); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	want := "-ERR bad  +INJECTED\r\n+a b\r\n"
	if got := buf.String(); got != want {
		t.Errorf("got %q; want %q", got, want)
	}
}
