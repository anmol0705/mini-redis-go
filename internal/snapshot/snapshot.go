// Package snapshot reads and writes point-in-time snapshots of the keyspace in
// a compact binary format (docs/formats.md).
//
// Concurrency: the functions are plain and hold no state. A snapshot is
// written from a store.Dump copy, so a background goroutine can encode it
// while the engine keeps serving; the engine only pays for the copy.
//
// Safety: Decode never trusts a length from the file. Strings are read in
// chunks, collections grow only as elements really arrive, and the CRC32
// trailer must match before anything is returned, so a corrupt or hostile file
// yields an error and never a half-loaded keyspace.
package snapshot

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/anmol0705/mini-redis-go/internal/store"
)

// Format constants; see docs/formats.md.
const (
	magic          = "MRDB"
	version        = 1
	tagEnd         = 0xFF
	maxStringLen   = 512 * 1024 * 1024
	readChunk      = 64 * 1024
	initialElems   = 1024
	trailerLen     = 4
	defaultFileExt = ".tmp"
)

// DefaultFileName is the snapshot file's default name inside the data directory.
const DefaultFileName = "dump.mrdb"

// ErrCorrupt wraps every error caused by bad snapshot content.
var ErrCorrupt = errors.New("snapshot: corrupt file")

func corrupt(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrCorrupt, fmt.Sprintf(format, a...))
}

// ---- encoding ----

type encoder struct {
	w   io.Writer
	crc hash.Hash32
	err error
	buf [8]byte
}

func (e *encoder) write(p []byte) {
	if e.err != nil {
		return
	}
	e.crc.Write(p) // hash.Hash.Write never returns an error
	_, e.err = e.w.Write(p)
}

func (e *encoder) u8(v byte) { e.buf[0] = v; e.write(e.buf[:1]) }
func (e *encoder) u16(v uint16) {
	binary.BigEndian.PutUint16(e.buf[:2], v)
	e.write(e.buf[:2])
}
func (e *encoder) u32(v uint32) {
	binary.BigEndian.PutUint32(e.buf[:4], v)
	e.write(e.buf[:4])
}
func (e *encoder) u64(v uint64) {
	binary.BigEndian.PutUint64(e.buf[:8], v)
	e.write(e.buf[:8])
}
func (e *encoder) str(s string) {
	e.u32(uint32(len(s)))
	e.write([]byte(s))
}
func (e *encoder) strs(ss []string) {
	e.u32(uint32(len(ss)))
	for _, s := range ss {
		e.str(s)
	}
}

// Encode writes items as a snapshot to w, ending with the CRC32 trailer.
func Encode(w io.Writer, items []store.Item) error {
	e := &encoder{w: w, crc: crc32.NewIEEE()}
	e.write([]byte(magic))
	e.u16(version)
	e.u16(0) // flags, reserved

	for _, it := range items {
		e.u8(byte(it.Kind))
		e.u64(uint64(it.ExpireAt))
		e.str(it.Key)
		switch it.Kind {
		case store.KindString:
			e.str(it.Str)
		case store.KindList:
			e.strs(it.List)
		case store.KindHash:
			if len(it.Hash)%2 != 0 {
				return fmt.Errorf("snapshot: hash %q has an odd number of strings", it.Key)
			}
			e.u32(uint32(len(it.Hash) / 2))
			for _, s := range it.Hash {
				e.str(s)
			}
		case store.KindSet:
			e.strs(it.Set)
		case store.KindZSet:
			e.u32(uint32(len(it.ZSet)))
			for _, m := range it.ZSet {
				e.u64(math.Float64bits(m.Score))
				e.str(m.Member)
			}
		default:
			return fmt.Errorf("snapshot: item %q has unknown kind %d", it.Key, it.Kind)
		}
		if e.err != nil {
			return e.err
		}
	}
	e.u8(tagEnd)

	// The trailer covers everything before it, so it is written directly.
	var tail [trailerLen]byte
	binary.BigEndian.PutUint32(tail[:], e.crc.Sum32())
	if e.err != nil {
		return e.err
	}
	_, err := w.Write(tail[:])
	return err
}

// ---- decoding ----

type decoder struct {
	r   *bufio.Reader
	crc hash.Hash32
	buf [8]byte
}

// fill reads exactly len(p) bytes and adds them to the checksum.
func (d *decoder) fill(p []byte) error {
	if _, err := io.ReadFull(d.r, p); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return corrupt("file ends early")
		}
		return err
	}
	d.crc.Write(p)
	return nil
}

func (d *decoder) u8() (byte, error) {
	err := d.fill(d.buf[:1])
	return d.buf[0], err
}
func (d *decoder) u16() (uint16, error) {
	err := d.fill(d.buf[:2])
	return binary.BigEndian.Uint16(d.buf[:2]), err
}
func (d *decoder) u32() (uint32, error) {
	err := d.fill(d.buf[:4])
	return binary.BigEndian.Uint32(d.buf[:4]), err
}
func (d *decoder) u64() (uint64, error) {
	err := d.fill(d.buf[:8])
	return binary.BigEndian.Uint64(d.buf[:8]), err
}

// str reads a length-prefixed string. The announced length is not trusted:
// big strings are read chunk by chunk so a lying header costs only the bytes
// that really exist.
func (d *decoder) str() (string, error) {
	n, err := d.u32()
	if err != nil {
		return "", err
	}
	if n > maxStringLen {
		return "", corrupt("string of %d bytes exceeds the limit", n)
	}
	if n <= readChunk {
		b := make([]byte, n)
		if err := d.fill(b); err != nil {
			return "", err
		}
		return string(b), nil
	}
	var out []byte
	chunk := make([]byte, readChunk)
	for remaining := int(n); remaining > 0; {
		k := min(remaining, readChunk)
		if err := d.fill(chunk[:k]); err != nil {
			return "", err
		}
		out = append(out, chunk[:k]...)
		remaining -= k
	}
	return string(out), nil
}

// strs reads count strings; the slice grows as they arrive.
func (d *decoder) strs(count uint32) ([]string, error) {
	out := make([]string, 0, min(int(count), initialElems))
	for range count {
		s, err := d.str()
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// Decode reads a snapshot, validates it completely and returns its items.
// Any damage (bad magic, unknown version, truncation, invalid structure,
// checksum mismatch, trailing bytes) returns an error wrapping ErrCorrupt.
func Decode(r io.Reader) ([]store.Item, error) {
	d := &decoder{r: bufio.NewReaderSize(r, 64*1024), crc: crc32.NewIEEE()}

	head := make([]byte, len(magic))
	if err := d.fill(head); err != nil {
		return nil, err
	}
	if string(head) != magic {
		return nil, corrupt("bad magic %q", head)
	}
	ver, err := d.u16()
	if err != nil {
		return nil, err
	}
	if ver != version {
		return nil, corrupt("unsupported version %d", ver)
	}
	if _, err := d.u16(); err != nil { // flags
		return nil, err
	}

	var items []store.Item
	for {
		tag, err := d.u8()
		if err != nil {
			return nil, err
		}
		if tag == tagEnd {
			break
		}
		it, err := d.item(store.Kind(tag))
		if err != nil {
			return nil, err
		}
		items = append(items, it)
	}

	want := d.crc.Sum32() // checksum of everything before the trailer
	var tail [trailerLen]byte
	if _, err := io.ReadFull(d.r, tail[:]); err != nil {
		return nil, corrupt("missing checksum")
	}
	if got := binary.BigEndian.Uint32(tail[:]); got != want {
		return nil, corrupt("checksum mismatch (file %08x, computed %08x)", got, want)
	}
	if _, err := d.r.ReadByte(); !errors.Is(err, io.EOF) {
		return nil, corrupt("unexpected data after the checksum")
	}
	return items, nil
}

func (d *decoder) item(kind store.Kind) (store.Item, error) {
	it := store.Item{Kind: kind}
	exp, err := d.u64()
	if err != nil {
		return it, err
	}
	it.ExpireAt = int64(exp)
	if it.ExpireAt < 0 {
		return it, corrupt("negative expiry")
	}
	if it.Key, err = d.str(); err != nil {
		return it, err
	}

	switch kind {
	case store.KindString:
		it.Str, err = d.str()
	case store.KindList:
		it.List, err = d.collection(it.Key)
	case store.KindSet:
		it.Set, err = d.collection(it.Key)
	case store.KindHash:
		var n uint32
		if n, err = d.u32(); err != nil {
			return it, err
		}
		if n == 0 {
			return it, corrupt("empty hash %q", it.Key)
		}
		it.Hash = make([]string, 0, min(int(n)*2, initialElems))
		for range n {
			var f, v string
			if f, err = d.str(); err != nil {
				return it, err
			}
			if v, err = d.str(); err != nil {
				return it, err
			}
			it.Hash = append(it.Hash, f, v)
		}
	case store.KindZSet:
		var n uint32
		if n, err = d.u32(); err != nil {
			return it, err
		}
		if n == 0 {
			return it, corrupt("empty sorted set %q", it.Key)
		}
		it.ZSet = make([]store.ZMember, 0, min(int(n), initialElems))
		for range n {
			var bits uint64
			var m string
			if bits, err = d.u64(); err != nil {
				return it, err
			}
			if m, err = d.str(); err != nil {
				return it, err
			}
			score := math.Float64frombits(bits)
			if math.IsNaN(score) {
				return it, corrupt("NaN score in %q", it.Key)
			}
			it.ZSet = append(it.ZSet, store.ZMember{Member: m, Score: score})
		}
	default:
		return it, corrupt("unknown entry tag %d", kind)
	}
	return it, err
}

// collection reads a count followed by that many strings; empty collections
// cannot exist in a keyspace, so a zero count is corruption.
func (d *decoder) collection(key string) ([]string, error) {
	n, err := d.u32()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, corrupt("empty collection %q", key)
	}
	return d.strs(n)
}

// ---- files ----

// WriteFile writes items to path atomically: the data goes to a temporary
// file in the same directory, is fsynced, and only then renamed over path, so
// a crash leaves either the old snapshot or the new one, never half of one.
func WriteFile(path string, items []store.Item) (err error) {
	tmp := path + defaultFileExt
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("snapshot: create temp file: %w", err)
	}
	defer func() {
		if err != nil {
			_ = f.Close() // already failing; the original error is the useful one
			_ = os.Remove(tmp)
		}
	}()

	bw := bufio.NewWriterSize(f, 256*1024)
	if err = Encode(bw, items); err != nil {
		return err
	}
	if err = bw.Flush(); err != nil {
		return fmt.Errorf("snapshot: write: %w", err)
	}
	if err = f.Sync(); err != nil {
		return fmt.Errorf("snapshot: fsync: %w", err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("snapshot: close: %w", err)
	}
	if err = os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("snapshot: rename: %w", err)
	}
	syncDir(filepath.Dir(path))
	return nil
}

// ReadFile reads and validates the snapshot at path.
func ReadFile(path string) ([]store.Item, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }() // read-only file: a close error cannot lose data
	items, err := Decode(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return items, nil
}

// syncDir makes the rename durable where the platform supports fsync on a
// directory; elsewhere (Windows) the error is ignored.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync() // best-effort durability step
	_ = d.Close()
}
