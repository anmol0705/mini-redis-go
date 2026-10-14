package resp

import (
	"bufio"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
)

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    []string
		wantErr error
	}{
		{"ping", "*1\r\n$4\r\nPING\r\n", []string{"PING"}, nil},
		{"set", "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$5\r\nAnmol\r\n", []string{"SET", "name", "Anmol"}, nil},
		{"empty value", "*2\r\n$3\r\nGET\r\n$0\r\n\r\n", []string{"GET", ""}, nil},
		{"value containing CRLF", "*2\r\n$3\r\nGET\r\n$4\r\na\r\nb\r\n", []string{"GET", "a\r\nb"}, nil},
		{"clean EOF", "", nil, io.EOF},
		{"cut after first element", "*2\r\n$3\r\nGET\r\n", nil, io.ErrUnexpectedEOF},
		{"cut inside a value", "*1\r\n$4\r\nPI", nil, io.ErrUnexpectedEOF},
		{"no array marker", "PING\r\n", nil, ErrProtocol},
		{"non-numeric length", "*1\r\n$x\r\nPING\r\n", nil, ErrProtocol},
		{"negative length", "*1\r\n$-5\r\nabc\r\n", nil, ErrProtocol},
		{"missing CRLF after value", "*1\r\n$4\r\nPINGxx", nil, ErrProtocol},
		{"cut after array header", "*2\r\n", nil, io.ErrUnexpectedEOF},
		{"cut before value bytes", "*1\r\n$4\r\n", nil, io.ErrUnexpectedEOF},
		{"cut mid header line", "*1", nil, io.ErrUnexpectedEOF},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bufio.NewReader(strings.NewReader(tt.input))
			got, err := ReadCommand(r)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v; want %v", err, tt.wantErr)
			}
			if !slices.Equal(got, tt.want) {
				t.Errorf("got %q; want %q", got, tt.want)
			}
		})
	}
}

func TestReadCommandPipelined(t *testing.T) {
	input := "*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
	r := bufio.NewReader(strings.NewReader(input))

	got, err := ReadCommand(r)
	if err != nil || !slices.Equal(got, []string{"PING"}) {
		t.Fatalf("first command = %q, %v; want [PING], nil", got, err)
	}

	got, err = ReadCommand(r)
	if err != nil || !slices.Equal(got, []string{"GET", "k"}) {
		t.Fatalf("second command = %q, %v; want [GET k], nil", got, err)
	}

	if _, err = ReadCommand(r); !errors.Is(err, io.EOF) {
		t.Fatalf("third call err = %v; want io.EOF", err)
	}
}
