package resp

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"slices"
	"testing"
)

// FuzzReadCommand feeds arbitrary bytes to the reader. It must never panic,
// must only fail with the documented errors, and anything it accepts must
// survive an encode/decode round trip unchanged.
func FuzzReadCommand(f *testing.F) {
	seeds := []string{
		"*1\r\n$4\r\nPING\r\n",
		"*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nAnmol\r\n",
		"*2\r\n$3\r\nGET\r\n$0\r\n\r\n",
		"*1\r\n$4\r\nPI",
		"*2\r\n",
		"*1",
		"",
		"PING\r\n",
		"*-1\r\n",
		"*0\r\n",
		"*1\r\n$-1\r\n",
		"*1\r\n$536870913\r\n",
		"*1048577\r\n",
		"*1\r\n$4\r\nPINGxx",
		"*1\r\n$x\r\nPING\r\n",
		"*1\r\n$4\r\na\r\nb\r\n",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n*1\r\n$4\r\nPING\r\n",
		"\r\n\r\n*\r\n",
		"*99999999999999999999\r\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		r := bufio.NewReader(bytes.NewReader(data))
		for range 8 { // a few pipelined commands
			args, err := ReadCommand(r)
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, ErrProtocol) {
					t.Fatalf("unexpected error type: %v", err)
				}
				return
			}
			if len(args) == 0 {
				t.Fatal("ReadCommand returned an empty command without an error")
			}
			again, err := ReadCommand(bufio.NewReader(bytes.NewReader(AppendCommand(nil, args))))
			if err != nil || !slices.Equal(again, args) {
				t.Fatalf("round trip changed %q into %q (%v)", args, again, err)
			}
		}
	})
}
