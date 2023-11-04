package nitecache

import (
	"reflect"
	"testing"
)

func TestAutoCodecDetection(t *testing.T) {
	c, err := NewCache(Member{ID: "1", Addr: ":8000"}, []Member{})
	if err != nil {
		t.Fatal(err)
	}
	stringTable := NewTable[string]("potato").Build(c)

	str := "test"
	encodedStr, err := stringTable.codec.Encode(str)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(string(encodedStr), str) {
		t.Fatalf("expected %s, got %s", str, string(encodedStr))
	}

	bytesTable := NewTable[[]byte]("potato").Build(c)
	if err != nil {
		t.Fatal(err)
	}
	bytes := []byte("test")
	encodedBytes, err := bytesTable.codec.Encode(bytes)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(encodedBytes, bytes) {
		t.Fatalf("expected %s, got %s", string(bytes), string(encodedBytes))
	}

	mapsTable := NewTable[map[string]string]("potato").Build(c)
	if err != nil {
		t.Fatal(err)
	}
	expectedMap := []byte(`{"key":"value"}`)
	encodedMap, err := mapsTable.codec.Encode(map[string]string{"key": "value"})
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(encodedMap, expectedMap) {
		t.Fatalf("expected %s, got %s", string(expectedMap), string(encodedMap))
	}
}
