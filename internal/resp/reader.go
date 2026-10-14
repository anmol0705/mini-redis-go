package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// ErrProtocol means the client sent bytes that are not valid RESP.
var ErrProtocol = errors.New("resp: protocol error")

const (
	maxArrayLen = 1024 * 1024
	maxBulkLen  = 512 * 1024 * 1024
)

// ReadCommand reads one command (an array of bulk strings) from r.
// It returns io.EOF if r ends cleanly before a command starts, and
// io.ErrUnexpectedEOF if it ends in the middle of one.
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

	args := make([]string, n)
	for i := 0; i < n; i++ {
		args[i], err = readBulkString(r)
		if err != nil {
			// We are inside a command now, so a plain EOF means the
			// client disappeared mid-command.
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
	}
	return args, nil
}

// readLine reads up to and including "\r\n" and returns the line without it.
func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		// Some text arrived but no newline: a half-sent line, not a clean end.
		if errors.Is(err, io.EOF) && len(line) > 0 {
			return "", io.ErrUnexpectedEOF
		}
		return "", err
	}

	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", fmt.Errorf("%w: line not terminated by CRLF", ErrProtocol)
	}
	return line[:len(line)-2], nil
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

	// Read the data plus its trailing "\r\n" in one go.
	buf := make([]byte, length+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	if buf[length] != '\r' || buf[length+1] != '\n' {
		return "", fmt.Errorf("%w: bulk string not terminated by CRLF", ErrProtocol)
	}
	return string(buf[:length]), nil
}
