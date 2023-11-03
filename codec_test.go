package nitecache_test

import (
	"bytes"
	"context"
	"fmt"
	"github.com/MysteriousPotato/nitecache"
	"github.com/MysteriousPotato/nitecache/test_utils"
	"reflect"
	"testing"
)

type (
	codecTest[T any] struct {
		decoded T
		encoded []byte
	}
	Coord struct {
		x, y float64
	}
	CoordCodec struct{}
)

func (c CoordCodec) Decode(b []byte, v *Coord) error {
	buf := bytes.NewBuffer(b)
	_, err := fmt.Fscanln(buf, &v.x, &v.y)
	return err
}

func (c CoordCodec) Encode(v Coord) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := fmt.Fprintln(&buf, v.x, v.y); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func TestStringCodec(t *testing.T) {
	type strLike []byte
	strLikeTests := []codecTest[strLike]{
		{
			decoded: strLike("potato"),
			encoded: []byte("potato"),
		}, {
			decoded: strLike("'potato'"),
			encoded: []byte("'potato'"),
		}, {
			decoded: strLike("123"),
			encoded: []byte("123"),
		},
	}
	strLikeCodec := nitecache.StringCodec[strLike]{}

	for _, strTest := range strLikeTests {
		encoded, err := strLikeCodec.Encode(strTest.decoded)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(encoded, strTest.encoded) {
			t.Errorf("expected str encoded: %v, got: %v", strTest.encoded, encoded)
		}

		var decoded strLike
		if err = strLikeCodec.Decode(strTest.encoded, &decoded); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(decoded, strTest.decoded) {
			t.Errorf("expected str decoded: %v, got: %v", strTest.decoded, decoded)
		}
	}
}

func TestCustomCodec(t *testing.T) {
	ctx := context.Background()
	self := nitecache.Member{ID: "1", Addr: test.GetUniqueAddr()}
	c, err := nitecache.NewCache(self, []nitecache.Member{self})
	if err != nil {
		t.Fatal(err)
	}

	table := nitecache.NewTable[Coord]("coord").WithCodec(CoordCodec{}).Build(c)
	expected := Coord{
		x: 101.143,
		y: 32.766,
	}
	if err = table.Put(ctx, "test", expected, 0); err != nil {
		t.Fatal(err)
	}

	v, err := table.Get(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(v, expected) {
		t.Errorf("expected: %+v, got: %+v", expected, v)
	}
}
