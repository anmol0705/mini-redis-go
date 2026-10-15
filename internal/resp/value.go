package resp

import "fmt"

// Kind says which RESP type a Value holds.
type Kind int

// The zero Kind is deliberately not valid, so an unset Value is caught
// instead of silently turning into an empty string.
const (
	KindSimpleString Kind = iota + 1
	KindError
	KindInteger
	KindBulkString
	KindNull
)

// Value is a reply as plain data. Handlers build one; the server writes it.
type Value struct {
	Kind Kind
	Str  string // used by simple strings, errors and bulk strings
	Int  int64  // used by integers
}

// SimpleString returns a status reply such as OK.
func SimpleString(s string) Value { return Value{Kind: KindSimpleString, Str: s} }

// Error returns an error reply such as "ERR unknown command".
func Error(msg string) Value { return Value{Kind: KindError, Str: msg} }

// Integer returns an integer reply.
func Integer(n int64) Value { return Value{Kind: KindInteger, Int: n} }

// BulkString returns a string reply.
func BulkString(s string) Value { return Value{Kind: KindBulkString, Str: s} }

// Null returns the "no such value" reply.
func Null() Value { return Value{Kind: KindNull} }

// WriteValue writes v in RESP format.
func (w *Writer) WriteValue(v Value) error {
	switch v.Kind {
	case KindSimpleString:
		return w.WriteSimpleString(v.Str)
	case KindError:
		return w.WriteError(v.Str)
	case KindInteger:
		return w.WriteInteger(v.Int)
	case KindBulkString:
		return w.WriteBulkString(v.Str)
	case KindNull:
		return w.WriteNull()
	default:
		return fmt.Errorf("resp: unknown value kind %d", v.Kind)
	}
}
