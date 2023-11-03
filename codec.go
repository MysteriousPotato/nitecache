package nitecache

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
)

type (
	// Codec defines the interface to implement custom marshalling/unmarshalling
	//
	// If a generic type of string or []byte is supplied to the table, nitecache will automatically use [StringCodec]
	// Otherwise nitecache will default to [JsonCodec].
	//
	// Example:
	//
	//	type (
	//		Coord struct {
	//			x, y float64
	//		}
	//		CoordCodec struct{}
	//	)
	//
	//	func (c CoordCodec) Decode(b []byte, v *Coord) error {
	//		buf := bytes.NewBuffer(b)
	//		_, err := fmt.Fscanln(buf, &v.x, &v.y)
	//		return err
	//	}
	//
	//	func (c CoordCodec) Encode(v Coord) ([]byte, error) {
	//		var buf bytes.Buffer
	//		if _, err := fmt.Fprintln(&buf, v.x, v.y); err != nil {
	//			return nil, err
	//		}
	//		return buf.Bytes(), nil
	//	}
	Codec[T any] interface {
		Encode(value T) ([]byte, error)
		Decode(bytes []byte, value *T) error
	}
	// StringCodec implements [Codec] by preventing unnecessary marshalling/unmarshalling for string-like tables.
	//
	// This codec will automatically be used for string/[]byte tables.
	// However, nitecache won't use it for string/[]byte alias, so you'll have to supply the codec yourself.
	StringCodec[T ~string | ~[]byte] struct{}
	// JsonCodec implements [Codec] using [encoding/json]
	//
	// This is the default codec for all type except []byte and string.
	JsonCodec[T any] struct{}
	// GobCodec implements [Codec] using [encoding/gob]
	GobCodec[T any] struct{}
)

func (c StringCodec[T]) Decode(b []byte, v *T) error {
	*v = T(b)
	return nil
}

func (c StringCodec[T]) Encode(v T) ([]byte, error) {
	return []byte(v), nil
}

func (c JsonCodec[T]) Decode(b []byte, v *T) error {
	return json.Unmarshal(b, v)
}

func (c JsonCodec[T]) Encode(v T) ([]byte, error) {
	return json.Marshal(v)
}

func (c GobCodec[T]) Decode(b []byte, v *T) error {
	buf := bytes.NewBuffer(b)
	return gob.NewDecoder(buf).Decode(v)
}

func (c GobCodec[T]) Encode(v T) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(v)
	return buf.Bytes(), err
}
