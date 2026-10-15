// Package resp reads commands and writes replies in the RESP2 wire format
// used by Redis.
//
// Concurrency: a Reader function or Writer is NOT safe for concurrent use.
// Each network connection owns exactly one bufio.Reader and one Writer and
// uses them from a single goroutine.
package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// ErrProtocol means the client sent bytes that are not valid RESP.
var ErrProtocol = errors.New("resp: protocol error")

// Limits protect the server from sizes announced by the network. A client
// can claim anything in a header, so we never trust a number until the data
// behind it has actually arrived.
const (
	// maxLineLen caps a header line ("*3", "$1234") including its CRLF.
	maxLineLen = 64 * 1024
	// maxArrayLen is the most arguments one command may have.
	maxArrayLen = 1024 * 1024
	// maxBulkLen is the largest single argument (the same cap Redis uses).
	maxBulkLen = 512 * 1024 * 1024
	// readChunk is how much of a large bulk string we read at a time, so
	// memory grows with the bytes that really arrive.
	readChunk = 64 * 1024
	// initialArgs caps the slice preallocated for a command's arguments.
	initialArgs = 64
)

// ReadCommand reads one command (an array of bulk strings) from r.
// It returns io.EOF if r ends cleanly before a command starts, and
// io.ErrUnexpectedEOF if it ends in the middle of one. Any other
// malformed input returns an error wrapping ErrProtocol; after that the
// stream is out of sync and the caller should close the connection.
func ReadCommand(r *bufio.Reader) ([]string, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("%w: expected array header", ErrProtocol)
	}

	n, err := strconv.Atoi(line[1:])
	if err != nil || n < 1 || n > maxArrayLen {
		return nil, fmt.Errorf("%w: invalid array length", ErrProtocol)
	}

	// n came from the network: a 10-byte header must not buy a
	// multi-megabyte allocation. Start small and let append grow it.
	args := make([]string, 0, min(n, initialArgs))
	for range n {
		arg, err := readBulkString(r)
		if err != nil {
			// We are inside a command now, so a plain EOF means the
			// client disappeared mid-command.
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
		args = append(args, arg)
	}
	return args, nil
}

// readLine reads up to and including "\r\n" and returns the line without it.
// Lines longer than maxLineLen are rejected without buffering the rest.
func readLine(r *bufio.Reader) (string, error) {
	// ReadSlice returns a view into bufio's internal buffer that the next
	// read overwrites. For lines that fit in the buffer (the normal case)
	// we copy once at the end; longer lines are accumulated in acc.
	var acc []byte
	for {
		frag, err := r.ReadSlice('\n')
		if len(acc)+len(frag) > maxLineLen {
			return "", fmt.Errorf("%w: line too long", ErrProtocol)
		}
		switch {
		case err == nil:
			line := frag
			if acc != nil {
				line = append(acc, frag...)
			}
			if len(line) < 2 || line[len(line)-2] != '\r' {
				return "", fmt.Errorf("%w: line not terminated by CRLF", ErrProtocol)
			}
			return string(line[:len(line)-2]), nil
		case errors.Is(err, bufio.ErrBufferFull):
			acc = append(acc, frag...)
		case errors.Is(err, io.EOF) && len(acc)+len(frag) > 0:
			// Some text arrived but no newline: a half-sent line,
			// not a clean end.
			return "", io.ErrUnexpectedEOF
		default:
			return "", err
		}
	}
}

// readBulkString reads "$<len>\r\n<bytes>\r\n" and returns the bytes.
func readBulkString(r *bufio.Reader) (string, error) {
	line, err := readLine(r)
	if err != nil {
		return "", err
	}
	if len(line) == 0 || line[0] != '$' {
		return "", fmt.Errorf("%w: expected bulk string header", ErrProtocol)
	}

	length, err := strconv.Atoi(line[1:])
	if err != nil || length < 0 || length > maxBulkLen {
		return "", fmt.Errorf("%w: invalid bulk string length", ErrProtocol)
	}

	var data string
	if length <= readChunk {
		buf := make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", err
		}
		data = string(buf)
	} else if data, err = readLarge(r, length); err != nil {
		return "", err
	}

	var crlf [2]byte
	if _, err := io.ReadFull(r, crlf[:]); err != nil {
		return "", err
	}
	if crlf[0] != '\r' || crlf[1] != '\n' {
		return "", fmt.Errorf("%w: bulk string not terminated by CRLF", ErrProtocol)
	}
	return data, nil
}

// readLarge reads exactly length bytes in chunks. A client announcing a
// 512 MiB value and then sending nothing costs us one chunk, not 512 MiB.
// strings.Builder lets us hand back the result without a final copy.
func readLarge(r io.Reader, length int) (string, error) {
	var sb strings.Builder
	chunk := make([]byte, readChunk)
	for remaining := length; remaining > 0; {
		n := min(remaining, readChunk)
		if _, err := io.ReadFull(r, chunk[:n]); err != nil {
			return "", err
		}
		sb.Write(chunk[:n]) // strings.Builder.Write never returns an error
		remaining -= n
	}
	return sb.String(), nil
}
