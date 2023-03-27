package nitecache

import "encoding/json"

type Codec[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(bytes []byte, value *T) error
}

type jsonCodec[T any] struct{}

func (c jsonCodec[T]) Decode(b []byte, v *T) error {
	return json.Unmarshal(b, v)
}

func (c jsonCodec[T]) Encode(v T) ([]byte, error) {
	return json.Marshal(v)
}
