package snapshot

import (
	"bytes"
	"reflect"
	"testing"
)

// FuzzDecode runs the snapshot loader on garbage. It must never panic or hang,
// must return an error for damaged input, and anything it accepts must
// re-encode to a snapshot that decodes to the same items.
func FuzzDecode(f *testing.F) {

	var buf bytes.Buffer
	if err := Encode(&buf, sampleItems()); err != nil {
		f.Fatal(err)
	}
	good := buf.Bytes()
	var empty bytes.Buffer
	if err := Encode(&empty, nil); err != nil {
		f.Fatal(err)
	}

	for _, s := range [][]byte{
		good,
		empty.Bytes(),
		good[:len(good)/2],
		good[:len(good)-1],
		append(bytes.Clone(good), 0),
		[]byte("MRDB"),
		[]byte("MRDB\x00\x01\x00\x00\xff"),
		[]byte("MRDB\x00\x01\x00\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01k\xff\xff\xff\xff"),
		{},
		{0xff, 0xff, 0xff},
	} {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		items, err := Decode(bytes.NewReader(data))
		if err != nil {
			return
		}
		var out bytes.Buffer
		if err := Encode(&out, items); err != nil {
			t.Fatalf("accepted input cannot be re-encoded: %v", err)
		}
		again, err := Decode(&out)
		if err != nil {
			t.Fatalf("re-encoded snapshot rejected: %v", err)
		}
		if len(items) != len(again) || (len(items) > 0 && !reflect.DeepEqual(items, again)) {
			t.Fatal("re-encoding changed the items")
		}
	})
}
