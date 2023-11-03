package inmem

type (
	Opt  func(*opts)
	opts struct {
		skipInc bool
	}
)

// SkipInc specify whether the "count" used to apply the LFU/LRU policies must be incremented.
//
// # Defaults to false
//
// This is useful when multiple get/put operations must count as a single access.
func SkipInc(skip bool) Opt {
	return func(o *opts) { o.skipInc = skip }
}

func getOpts(options ...Opt) *opts {
	defaultOpts := &opts{}
	for _, o := range options {
		o(defaultOpts)
	}
	return defaultOpts
}
