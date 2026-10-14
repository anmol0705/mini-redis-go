package resp

import (
	"bufio"
	"fmt"
	"io"
)

// Writer encodes replies in the RESP format.
type Writer struct {
	w *bufio.Writer
}

// NewWriter returns a Writer that buffers output to w. Call Flush to send it.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(w)}
}

// WriteSimpleString writes a short status reply such as OK.
func (w *Writer) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(w.w, "+%s\r\n", s)
	return err
}

// WriteError writes an error reply such as "ERR unknown command".
func (w *Writer) WriteError(msg string) error {
	_, err := fmt.Fprintf(w.w, "-%s\r\n", msg)
	return err
}

// WriteInteger writes an integer reply.
func (w *Writer) WriteInteger(n int64) error {
	_, err := fmt.Fprintf(w.w, ":%d\r\n", n)
	return err
}

// WriteBulkString writes a length-prefixed string.
func (w *Writer) WriteBulkString(s string) error {
	_, err := fmt.Fprintf(w.w, "$%d\r\n%s\r\n", len(s), s)
	return err
}

// WriteNull writes the "no such value" reply.
func (w *Writer) WriteNull() error {
	_, err := w.w.WriteString("$-1\r\n")
	return err
}

// Flush sends everything buffered so far.
func (w *Writer) Flush() error {
	return w.w.Flush()
}
