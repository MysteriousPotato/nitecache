package nitecache

type Members []Member

type Member struct {
	ID   string
	Addr string
}

func (m Member) isZero() bool {
	return m == (Member{})
}

func (ms Members) equals(others Members) bool {
	if len(ms) != len(others) {
		return false
	}

	hashMap := make(map[string]any, len(ms))
	for _, m := range ms {
		hashMap[m.ID] = nil
	}

	for _, other := range others {
		if _, ok := hashMap[other.ID]; !ok {
			return false
		}
	}
	return true
}
