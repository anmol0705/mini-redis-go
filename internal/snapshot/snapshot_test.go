package snapshot

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/anmol0705/mini-redis-go/internal/store"
)

func sampleItems() []store.Item {
	return []store.Item{
		{Key: "str", Kind: store.KindString, Str: "hello"},
		{Key: "", Kind: store.KindString, Str: ""}, // empty key and value are legal
		{Key: "bin\x00\xff\r\n", Kind: store.KindString, Str: "\x00\x01\x02 binary \xff", ExpireAt: 1_700_000_123_456},
		{Key: "list", Kind: store.KindList, List: []string{"a", "", "c"}, ExpireAt: 5},
		{Key: "hash", Kind: store.KindHash, Hash: []string{"f1", "v1", "f2", ""}},
		{Key: "set", Kind: store.KindSet, Set: []string{"x", "y", "z"}},
		{Key: "zset", Kind: store.KindZSet, ZSet: []store.ZMember{
			{Member: "neg", Score: -1.5}, {Member: "zero", Score: 0}, {Member: "pi", Score: math.Pi},
			{Member: "big", Score: 1e300}, {Member: "inf", Score: math.Inf(1)}, {Member: "-inf", Score: math.Inf(-1)},
		}},
	}
}

func encode(t *testing.T, items []store.Item) []byte {
	t.Helper()
	var b bytes.Buffer
	if err := Encode(&b, items); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return b.Bytes()
}

func TestRoundTripEveryType(t *testing.T) {
	want := sampleItems()
	got, err := Decode(bytes.NewReader(encode(t, want)))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round trip changed the data:\n got %+v\nwant %+v", got, want)
	}
}

func TestRoundTripEmptyAndLarge(t *testing.T) {
	if got, err := Decode(bytes.NewReader(encode(t, nil))); err != nil || len(got) != 0 {
		t.Errorf("empty snapshot: %v, %v", got, err)
	}
	// Bigger than one read chunk, and not a multiple of it.
	big := strings.Repeat("0123456789", 20_000) + "tail"
	items := []store.Item{{Key: "big", Kind: store.KindString, Str: big}}
	got, err := Decode(bytes.NewReader(encode(t, items)))
	if err != nil || len(got) != 1 || got[0].Str != big {
		t.Errorf("large value did not survive: err=%v", err)
	}
}

func TestEncodingIsDeterministic(t *testing.T) {
	a, b := encode(t, sampleItems()), encode(t, sampleItems())
	if !bytes.Equal(a, b) {
		t.Error("encoding the same items twice gives different bytes")
	}
}

func TestEncodeRejectsBadItems(t *testing.T) {
	for _, it := range []store.Item{
		{Key: "k", Kind: 0},
		{Key: "k", Kind: store.KindHash, Hash: []string{"odd"}},
	} {
		if err := Encode(&bytes.Buffer{}, []store.Item{it}); err == nil {
			t.Errorf("Encode accepted %+v", it)
		}
	}
}

type failAfter struct{ n int }

func (f *failAfter) Write(p []byte) (int, error) {
	if f.n -= len(p); f.n < 0 {
		return 0, errors.New("disk full")
	}
	return len(p), nil
}

func TestEncodeReportsWriteErrors(t *testing.T) {
	size := len(encode(t, sampleItems()))
	for _, limit := range []int{0, 10, size / 2, size - 2} {
		if err := Encode(&failAfter{n: limit}, sampleItems()); err == nil {
			t.Errorf("no error when the writer fails after %d bytes", limit)
		}
	}
}

// Flipping any single byte anywhere must be detected.
func TestEverySingleByteCorruptionIsDetected(t *testing.T) {
	data := encode(t, sampleItems())
	for i := range data {
		mut := bytes.Clone(data)
		mut[i] ^= 0x55
		if _, err := Decode(bytes.NewReader(mut)); err == nil {
			t.Fatalf("flipping byte %d went unnoticed", i)
		} else if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("byte %d: error %v does not wrap ErrCorrupt", i, err)
		}
	}
}

// Cutting the file anywhere must be detected.
func TestEveryTruncationIsDetected(t *testing.T) {
	data := encode(t, sampleItems())
	for n := range data {
		if _, err := Decode(bytes.NewReader(data[:n])); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("truncation to %d bytes: err = %v; want ErrCorrupt", n, err)
		}
	}
}

func TestTrailingGarbageIsRejected(t *testing.T) {
	data := append(encode(t, sampleItems()), 'x')
	if _, err := Decode(bytes.NewReader(data)); !errors.Is(err, ErrCorrupt) {
		t.Errorf("err = %v", err)
	}
}

// body builds a snapshot file from raw entry bytes with a VALID checksum, so
// the structural checks (not the CRC) are what must catch the problem.
func body(entries ...[]byte) []byte {
	b := []byte("MRDB\x00\x01\x00\x00")
	for _, e := range entries {
		b = append(b, e...)
	}
	b = append(b, tagEnd)
	return binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
}

func u32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }
func u64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }
func cat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}
func s(v string) []byte { return cat(u32(uint32(len(v))), []byte(v)) }

func TestStructuralValidation(t *testing.T) {
	str := byte(store.KindString)
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{"bad magic", append([]byte("XXXX\x00\x01\x00\x00\xff"), 0, 0, 0, 0), "bad magic"},
		{"future version", cat([]byte("MRDB\x00\x63\x00\x00\xff"), u32(0)), "unsupported version"},
		{"unknown tag", body(cat([]byte{9}, u64(0), s("k"))), "unknown entry tag"},
		{"negative expiry", body(cat([]byte{str}, u64(1<<63), s("k"), s("v"))), "negative expiry"},
		{"empty list", body(cat([]byte{byte(store.KindList)}, u64(0), s("k"), u32(0))), "empty collection"},
		{"empty set", body(cat([]byte{byte(store.KindSet)}, u64(0), s("k"), u32(0))), "empty collection"},
		{"empty hash", body(cat([]byte{byte(store.KindHash)}, u64(0), s("k"), u32(0))), "empty hash"},
		{"empty zset", body(cat([]byte{byte(store.KindZSet)}, u64(0), s("k"), u32(0))), "empty sorted set"},
		{"NaN score", body(cat([]byte{byte(store.KindZSet)}, u64(0), s("k"), u32(1), u64(math.Float64bits(math.NaN())), s("m"))), "NaN"},
		{"string over the limit", body(cat([]byte{str}, u64(0), s("k"), u32(maxStringLen+1))), "exceeds the limit"},
		{"missing end marker", cat([]byte("MRDB\x00\x01\x00\x00")), "ends early"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Decode(bytes.NewReader(tt.data))
			if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v; want ErrCorrupt containing %q", err, tt.want)
			}
		})
	}
	// Sanity: the helper really builds a loadable file.
	if _, err := Decode(bytes.NewReader(body(cat([]byte{str}, u64(0), s("k"), s("v"))))); err != nil {
		t.Errorf("valid hand-built snapshot rejected: %v", err)
	}
}

// A header may claim a gigantic string; memory must follow the real bytes.
func TestLyingLengthDoesNotAllocateTheClaim(t *testing.T) {
	data := cat([]byte("MRDB\x00\x01\x00\x00"), []byte{byte(store.KindString)}, u64(0), s("k"), u32(maxStringLen), []byte("only a little data"))

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := Decode(bytes.NewReader(data))
	runtime.ReadMemStats(&after)

	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v", err)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 4<<20 {
		t.Errorf("allocated %d bytes for a tiny file", grew)
	}
}

func TestLyingCountDoesNotPreallocate(t *testing.T) {
	data := cat([]byte("MRDB\x00\x01\x00\x00"), []byte{byte(store.KindList)}, u64(0), s("k"), u32(math.MaxUint32), s("one"))
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := Decode(bytes.NewReader(data))
	runtime.ReadMemStats(&after)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v", err)
	}
	if grew := after.TotalAlloc - before.TotalAlloc; grew > 4<<20 {
		t.Errorf("allocated %d bytes for a tiny file", grew)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("device error") }

func TestDecodeSurfacesIOErrors(t *testing.T) {
	_, err := Decode(errReader{})
	if err == nil || errors.Is(err, ErrCorrupt) {
		t.Errorf("err = %v; want the underlying I/O error", err)
	}
}

// ---- files ----

func TestWriteFileReadFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), DefaultFileName)
	want := sampleItems()
	if err := WriteFile(path, want); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("temp file left behind")
	}
	got, err := ReadFile(path)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("ReadFile = %+v, %v", got, err)
	}
}

func TestWriteFileReplacesAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), DefaultFileName)
	old := []store.Item{{Key: "old", Kind: store.KindString, Str: "1"}}
	if err := WriteFile(path, old); err != nil {
		t.Fatal(err)
	}

	// A failing write must leave the previous snapshot intact.
	bad := []store.Item{{Key: "new", Kind: store.KindString, Str: "2"}, {Key: "bad", Kind: 0}}
	if err := WriteFile(path, bad); err == nil {
		t.Fatal("WriteFile accepted an invalid item")
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Error("failed write left a temp file")
	}
	got, err := ReadFile(path)
	if err != nil || len(got) != 1 || got[0].Key != "old" {
		t.Errorf("old snapshot damaged: %+v, %v", got, err)
	}

	// A successful write replaces it.
	if err := WriteFile(path, sampleItems()); err != nil {
		t.Fatal(err)
	}
	if got, _ := ReadFile(path); len(got) != len(sampleItems()) {
		t.Errorf("replacement not visible: %d items", len(got))
	}
}

func TestWriteFileErrors(t *testing.T) {
	if err := WriteFile(filepath.Join(t.TempDir(), "missing", "dir", "x"), nil); err == nil {
		t.Error("WriteFile into a missing directory succeeded")
	}
}

func TestReadFileErrors(t *testing.T) {
	dir := t.TempDir()
	if _, err := ReadFile(filepath.Join(dir, "nope")); !os.IsNotExist(err) {
		t.Errorf("missing file: %v", err)
	}
	path := filepath.Join(dir, "garbage")
	os.WriteFile(path, []byte("this is not a snapshot"), 0o644)
	_, err := ReadFile(path)
	if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "garbage") {
		t.Errorf("garbage file: %v", err)
	}
}

// The snapshot must rebuild the same keyspace through the store.
func TestStoreRoundTripThroughSnapshot(t *testing.T) {
	const now = 1_700_000_000_000
	src := store.New(nil)
	src.SetNow(now)
	src.Set("a", "1", now+5000, false)
	src.Push("l", false, "x", "y")
	src.HSet("h", []string{"f", "v"})
	src.SAdd("s", []string{"m"})
	src.ZAdd("z", []store.ZMember{{Member: "m", Score: 2.5}})

	path := filepath.Join(t.TempDir(), DefaultFileName)
	if err := WriteFile(path, src.Dump()); err != nil {
		t.Fatal(err)
	}
	items, err := ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	dst := store.New(nil)
	dst.SetNow(now)
	if err := dst.Load(items); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(store.Canonical(dst.Dump()), store.Canonical(src.Dump())) {
		t.Error("keyspace differs after a snapshot round trip")
	}
}
