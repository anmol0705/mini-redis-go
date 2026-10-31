// Package aof implements the append-only file: a log of every successful
// write command in RESP form, replayed at startup to rebuild the data.
//
// Concurrency model (decision 007):
//
//   - Propagate, Commit, BeginRewrite, FinishRewrite and AbortRewrite are called
//     only from the engine goroutine, so the log's buffer and the rewrite
//     capture need no lock.
//   - Under the "everysec" policy one background goroutine fsyncs the file
//     once per second. It shares the file handle with the engine, so every
//     touch of the handle (write, fsync, swap during a rewrite) takes fmu.
//   - The syncer's failures are stored and returned by the next Commit, so a
//     failing disk is reported to clients instead of being lost.
//   - Close stops the syncer and waits for it.
package aof

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/anmol0705/mini-redis-go/internal/resp"
	"github.com/anmol0705/mini-redis-go/internal/store"
)

// FileName is the default name of the append-only file inside the data directory.
const FileName = "appendonly.aof"

// ErrRewriteInProgress is returned by BeginRewrite while a rewrite is running.
var ErrRewriteInProgress = errors.New("aof: rewrite already in progress")

// Policy says when the log is fsynced.
type Policy int

// The fsync policies (names match Redis's appendfsync).
const (
	// FsyncEverySec fsyncs in the background once per second: at most about
	// one second of writes can be lost in a crash.
	FsyncEverySec Policy = iota + 1
	// FsyncAlways fsyncs before a write is acknowledged: nothing acknowledged
	// is lost, at the cost of one fsync per request.
	FsyncAlways
	// FsyncNo never fsyncs; the operating system decides.
	FsyncNo
)

// String returns the Redis name of the policy.
func (p Policy) String() string {
	switch p {
	case FsyncEverySec:
		return "everysec"
	case FsyncAlways:
		return "always"
	case FsyncNo:
		return "no"
	}
	return "unknown"
}

// ParsePolicy converts "always", "everysec" or "no" into a Policy.
func ParsePolicy(s string) (Policy, error) {
	switch strings.ToLower(s) {
	case "always":
		return FsyncAlways, nil
	case "everysec":
		return FsyncEverySec, nil
	case "no":
		return FsyncNo, nil
	}
	return 0, fmt.Errorf("unknown appendfsync policy %q (want always, everysec or no)", s)
}

// File is the part of *os.File the log needs. Tests substitute a fake that
// counts Sync calls.
type File interface {
	io.Writer
	Sync() error
	Close() error
}

// Options configure a Log.
type Options struct {
	// Policy defaults to FsyncEverySec.
	Policy Policy
	// Path is the file's path; required for rewrites.
	Path string
	// Ticks supplies the once-per-second trigger for FsyncEverySec. It
	// returns the channel and a function that releases it. Tests inject their
	// own channel; the default is a time.Ticker of one second.
	Ticks func() (<-chan time.Time, func())
	// Open reopens the file after a rewrite (default: append to Path on disk).
	Open   func(path string) (File, error)
	Logger *slog.Logger
}

// Log is an open append-only file.
type Log struct {
	opts Options
	log  *slog.Logger

	fmu sync.Mutex // guards f
	f   File

	// Engine goroutine only:
	bw      *bufio.Writer
	enc     []byte
	capture []byte // commands logged since BeginRewrite

	rewriting atomic.Bool
	dirty     atomic.Bool // written since the last fsync (everysec)

	// size is the number of bytes written to the file so far (bytes still in
	// the bufio buffer are not counted). base is the size right after the
	// last rewrite, or the file size when the log was opened. Both are read
	// from any goroutine (INFO, the automatic rewrite check).
	size atomic.Int64
	base atomic.Int64

	errMu sync.Mutex
	bgErr error // last background fsync failure, cleared by a success

	quit      chan struct{}
	syncDone  chan struct{}
	closeOnce sync.Once
	closeErr  error
}

type lockedWriter struct{ l *Log }

func (w lockedWriter) Write(p []byte) (int, error) {
	w.l.fmu.Lock()
	defer w.l.fmu.Unlock()
	if w.l.f == nil {
		return 0, errors.New("aof: file is closed")
	}
	n, err := w.l.f.Write(p)
	w.l.size.Add(int64(n))
	return n, err
}

// New wraps an already open file. The Log owns f and closes it in Close.
func New(f File, opts Options) *Log {
	if opts.Policy == 0 {
		opts.Policy = FsyncEverySec
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Open == nil {
		opts.Open = openAppend
	}
	l := &Log{opts: opts, log: opts.Logger, f: f, quit: make(chan struct{}), syncDone: make(chan struct{})}
	l.bw = bufio.NewWriterSize(lockedWriter{l}, 64*1024)

	if opts.Policy == FsyncEverySec {
		var ch <-chan time.Time
		var release func()
		if opts.Ticks != nil {
			ch, release = opts.Ticks()
		} else {
			t := time.NewTicker(time.Second)
			ch, release = t.C, t.Stop
		}
		go l.syncLoop(ch, release)
	} else {
		close(l.syncDone)
	}
	return l
}

func openAppend(path string) (File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
}

// Open opens (creating if needed) the file at opts.Path for appending.
func Open(opts Options) (*Log, error) {
	f, err := openAppend(opts.Path)
	if err != nil {
		return nil, fmt.Errorf("aof: open: %w", err)
	}
	l := New(f, opts)
	// Nothing has been written yet, so the size can be set without a race.
	if st, err := os.Stat(opts.Path); err == nil {
		l.size.Store(st.Size())
		l.base.Store(st.Size())
	}
	return l, nil
}

// Size returns the bytes written to the log file so far. Safe from any goroutine.
func (l *Log) Size() int64 { return l.size.Load() }

// BaseSize returns the size the log had right after the last successful
// rewrite, or when it was opened. Safe from any goroutine.
func (l *Log) BaseSize() int64 { return l.base.Load() }

// syncLoop is the "everysec" goroutine; it exits when quit is closed.
func (l *Log) syncLoop(ticks <-chan time.Time, release func()) {
	defer close(l.syncDone)
	defer release()
	for {
		select {
		case <-ticks:
			l.syncIfDirty()
		case <-l.quit:
			return
		}
	}
}

func (l *Log) syncIfDirty() {
	if !l.dirty.Swap(false) {
		return
	}
	l.fmu.Lock()
	var err error
	if l.f != nil {
		err = l.f.Sync()
	}
	l.fmu.Unlock()

	l.errMu.Lock()
	l.bgErr = err
	l.errMu.Unlock()
	if err != nil {
		l.log.Error("background fsync of the append-only file failed", "err", err)
		l.dirty.Store(true) // try again next second
	}
}

func (l *Log) backgroundError() error {
	l.errMu.Lock()
	defer l.errMu.Unlock()
	if l.bgErr == nil {
		return nil
	}
	return fmt.Errorf("aof: background fsync failed: %w", l.bgErr)
}

// Propagate appends one command to the log's buffer (engine.Propagator).
func (l *Log) Propagate(cmd []string) {
	l.enc = resp.AppendCommand(l.enc[:0], cmd)
	// bufio.Writer remembers a write error and returns it from Flush.
	_, _ = l.bw.Write(l.enc)
	if l.rewriting.Load() {
		l.capture = append(l.capture, l.enc...)
	}
}

// Commit writes the buffered commands to the file and applies the fsync
// policy (engine.Propagator). With FsyncAlways the data is on disk when it
// returns nil.
func (l *Log) Commit() error {
	if err := l.bw.Flush(); err != nil {
		return fmt.Errorf("aof: write: %w", err)
	}
	switch l.opts.Policy {
	case FsyncAlways:
		return l.syncNow()
	case FsyncEverySec:
		l.dirty.Store(true)
		return l.backgroundError()
	}
	return nil
}

func (l *Log) syncNow() error {
	l.fmu.Lock()
	defer l.fmu.Unlock()
	if l.f == nil {
		return errors.New("aof: file is closed")
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("aof: fsync: %w", err)
	}
	return nil
}

// Rewriting reports whether a rewrite is in progress. Safe from any goroutine.
func (l *Log) Rewriting() bool { return l.rewriting.Load() }

// BeginRewrite starts capturing every command logged from now on, so none is
// lost while the caller writes the compacted file. The caller must take its
// snapshot of the data (store.DB.Dump) in the same engine step, so that the
// snapshot plus the captured commands equals the data.
func (l *Log) BeginRewrite() error {
	if !l.rewriting.CompareAndSwap(false, true) {
		return ErrRewriteInProgress
	}
	l.capture = l.capture[:0]
	return nil
}

// AbortRewrite discards the capture and removes tmp.
func (l *Log) AbortRewrite(tmp string) {
	l.capture = nil
	l.rewriting.Store(false)
	if tmp != "" {
		_ = os.Remove(tmp) // best effort: a leftover temp file is harmless
	}
}

// FinishRewrite completes a rewrite: it appends the captured commands to the
// compacted file tmp, fsyncs it, and atomically replaces the log with it.
// Writes made while the rewrite ran are therefore kept. On failure the old
// log keeps working and tmp is removed. It must run on the engine goroutine.
func (l *Log) FinishRewrite(tmp string) (err error) {
	defer func() {
		l.capture = nil
		l.rewriting.Store(false)
		if err != nil {
			_ = os.Remove(tmp)
		}
	}()
	if l.opts.Path == "" {
		return errors.New("aof: rewrite needs Options.Path")
	}
	// Everything logged so far is in the old file; make sure of it.
	if err := l.bw.Flush(); err != nil {
		return fmt.Errorf("aof: flush: %w", err)
	}

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("aof: open rewrite file: %w", err)
	}
	_, werr := f.Write(l.capture)
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return fmt.Errorf("aof: finish rewrite file: %w", werr)
	}
	st, statErr := os.Stat(tmp)

	l.fmu.Lock()
	defer l.fmu.Unlock()
	if l.f != nil {
		_ = l.f.Sync() // best effort; the new file already has everything
		_ = l.f.Close()
		l.f = nil
	}
	renameErr := os.Rename(tmp, l.opts.Path)
	// Reopen whichever file now lives at Path: the new one, or the old one if
	// the rename failed, so the log keeps working either way.
	nf, openErr := l.opts.Open(l.opts.Path)
	if openErr != nil {
		return fmt.Errorf("aof: reopen after rewrite: %w", errors.Join(renameErr, openErr))
	}
	l.f = nf
	if renameErr != nil {
		return fmt.Errorf("aof: replace file: %w", renameErr)
	}
	if statErr == nil {
		// The new file is the log now; later growth is measured from its size.
		l.size.Store(st.Size())
		l.base.Store(st.Size())
	}
	syncDir(filepath.Dir(l.opts.Path))
	return nil
}

// syncDir makes a rename durable on systems that support fsync on a
// directory. Where it is unsupported (Windows) the error is ignored.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync() // unsupported on some platforms; a best-effort durability step
	_ = d.Close()
}

// Close flushes, fsyncs and closes the file and stops the background syncer.
// It is idempotent. Call it after the engine has stopped.
func (l *Log) Close() error {
	l.closeOnce.Do(func() {
		close(l.quit)
		<-l.syncDone
		err := l.bw.Flush()
		l.fmu.Lock()
		defer l.fmu.Unlock()
		if l.f != nil {
			err = errors.Join(err, l.f.Sync(), l.f.Close())
			l.f = nil
		}
		l.closeErr = err
	})
	return l.closeErr
}

// ---- replay ----

// CorruptError reports damage in the middle of the file (as opposed to a
// torn final record, which is repaired).
type CorruptError struct {
	Offset int64 // byte offset of the bad record
	Err    error
}

// Error implements error, naming the byte offset of the damage.
func (e *CorruptError) Error() string {
	return fmt.Sprintf("aof: corrupt data at byte %d: %v", e.Offset, e.Err)
}

// Unwrap returns the underlying parse error, so errors.Is works through it.
func (e *CorruptError) Unwrap() error { return e.Err }

type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// Replay reads commands from r and passes each to apply, stopping at the end.
//
// valid is the number of bytes occupied by complete commands. If the data
// ends in the middle of a command (a crash during a write), torn is true and
// the caller should truncate the file to valid. Damage anywhere else returns
// a *CorruptError; an error from apply aborts the replay and is returned with
// the offset of the command.
func Replay(r io.Reader, apply func(args []string) error) (valid int64, torn bool, err error) {
	cr := &countingReader{r: r}
	br := bufio.NewReaderSize(cr, 64*1024)
	for {
		start := cr.n - int64(br.Buffered())
		args, rerr := resp.ReadCommand(br)
		switch {
		case rerr == nil:
			if aerr := apply(args); aerr != nil {
				return start, false, fmt.Errorf("aof: command at byte %d: %w", start, aerr)
			}
			valid = cr.n - int64(br.Buffered())
		case errors.Is(rerr, io.EOF):
			return valid, false, nil
		case errors.Is(rerr, io.ErrUnexpectedEOF):
			return valid, true, nil
		case errors.Is(rerr, resp.ErrProtocol):
			return start, false, &CorruptError{Offset: start, Err: rerr}
		default:
			return valid, false, rerr
		}
	}
}

// TruncateTo cuts the file at path to size bytes.
func TruncateTo(path string, size int64) error { return os.Truncate(path, size) }

// ---- compaction ----

// WriteItems writes the minimal commands that rebuild items: one SET per
// string, and chunked RPUSH/HSET/SADD/ZADD for collections, each followed by
// PEXPIREAT when the key has an expiry. Replaying the result gives the same
// data as the log it replaces.
func WriteItems(w io.Writer, items []store.Item) error {
	bw := bufio.NewWriterSize(w, 64*1024)
	var buf []byte
	emit := func(args []string) error {
		buf = resp.AppendCommand(buf[:0], args)
		_, err := bw.Write(buf)
		return err
	}
	const chunk = 256 // elements per command; keeps every command small

	for _, it := range items {
		var err error
		switch it.Kind {
		case store.KindString:
			args := []string{"SET", it.Key, it.Str}
			if it.ExpireAt != 0 {
				args = append(args, "PXAT", strconv.FormatInt(it.ExpireAt, 10))
			}
			err = emit(args)
			it.ExpireAt = 0 // already encoded in the SET
		case store.KindList:
			err = emitChunks(emit, "RPUSH", it.Key, it.List, 1, chunk)
		case store.KindHash:
			err = emitChunks(emit, "HSET", it.Key, it.Hash, 2, chunk)
		case store.KindSet:
			err = emitChunks(emit, "SADD", it.Key, it.Set, 1, chunk)
		case store.KindZSet:
			flat := make([]string, 0, 2*len(it.ZSet))
			for _, m := range it.ZSet {
				flat = append(flat, FormatScore(m.Score), m.Member)
			}
			err = emitChunks(emit, "ZADD", it.Key, flat, 2, chunk)
		default:
			err = fmt.Errorf("aof: cannot write item %q of kind %d", it.Key, it.Kind)
		}
		if err == nil && it.ExpireAt != 0 {
			err = emit([]string{"PEXPIREAT", it.Key, strconv.FormatInt(it.ExpireAt, 10)})
		}
		if err != nil {
			return err
		}
	}
	return bw.Flush()
}

// emitChunks writes "CMD key elems..." in pieces of at most chunk groups,
// where a group is width consecutive strings (2 for field/value pairs).
func emitChunks(emit func([]string) error, cmd, key string, elems []string, width, chunk int) error {
	step := width * chunk
	for i := 0; i < len(elems); i += step {
		end := min(i+step, len(elems))
		args := make([]string, 0, 2+end-i)
		args = append(args, cmd, key)
		args = append(args, elems[i:end]...)
		if err := emit(args); err != nil {
			return err
		}
	}
	return nil
}

// FormatScore prints a sorted-set score so that parsing it gives back
// exactly the same float64.
func FormatScore(f float64) string {
	switch {
	case math.IsInf(f, 1):
		return "inf"
	case math.IsInf(f, -1):
		return "-inf"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}
