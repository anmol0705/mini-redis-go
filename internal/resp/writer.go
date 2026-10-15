package resp

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// lineSafe replaces CR and LF, which would end a simple string or error
// early and let text smuggle in extra replies (protocol injection).
var lineSafe = strings.NewReplacer("\r", " ", "\n", " ")

func sanitizeLine(s string) string {
	if !strings.ContainsAny(s, "\r\n") {
		return s
	}
	return lineSafe.Replace(s)
}

// Writer encodes replies in the RESP format.
type Writer struct {
	w *bufio.Writer
}

// NewWriter returns a Writer that buffers output to w. Call Flush to send it.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: bufio.NewWriter(w)}
}

// WriteSimpleString writes a short status reply such as OK. CR and LF in s
// are replaced by spaces so a bad caller cannot break the framing.
func (w *Writer) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(w.w, "+%s\r\n", sanitizeLine(s))
	return err
}

// WriteError writes an error reply such as "ERR unknown command". CR and LF
// in msg are replaced by spaces for the same reason as WriteSimpleString.
func (w *Writer) WriteError(msg string) error {
	_, err := fmt.Fprintf(w.w, "-%s\r\n", sanitizeLine(msg))
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

// WriteArrayHeader writes the "*<n>" line that starts an array of n
// elements. The caller must then write exactly n values.
func (w *Writer) WriteArrayHeader(n int) error {
	_, err := fmt.Fprintf(w.w, "*%d\r\n", n)
	return err
}

// Flush sends everything buffered so far.
func (w *Writer) Flush() error {
	return w.w.Flush()
}
