package nitecache

import (
	"testing"
)

func assertRing(t *testing.T, r *hashring, members []Member) {
	if !r.members.equals(members) {
		t.Errorf("expected: %v Members, got: %v Members", members, r.members)
	}

	gotPointsLen := len(r.points)
	expectedPointsLen := len(r.members) * r.virtualNodes
	if gotPointsLen != expectedPointsLen {
		t.Errorf("expected: %v points, got: %v points", expectedPointsLen, gotPointsLen)
	}
}

func TestRing_NewOK(t *testing.T) {
	mTest := []Member{
		{ID: "node-1"},
		{ID: "node-2"},
		{ID: "node-3"},
	}
	cfg := ringCfg{
		Members:      mTest,
		VirtualNodes: 10,
	}

	ring, err := newRing(cfg)
	if err != nil {
		t.Error(err)
	}
	assertRing(t, ring, mTest)
}

func TestRing_setMembers(t *testing.T) {
	mTest := []Member{
		{ID: "node-1"},
		{ID: "node-2"},
		{ID: "node-3"},
	}
	cfg := ringCfg{
		Members:      mTest,
		VirtualNodes: 10,
	}

	ring, err := newRing(cfg)
	if err != nil {
		t.Error(err)
	}

	mTest[0].ID = "node-4"
	mTest = append(mTest, Member{ID: "node-5"})
	if err := ring.setMembers(mTest); err != nil {
		t.Error(err)
	}
	assertRing(t, ring, mTest)

	mTest = []Member{
		{ID: "node-1"},
		{ID: "node-2"},
		{ID: "node-3"},
	}
	if err := ring.setMembers(mTest); err != nil {
		t.Error(err)
	}
	assertRing(t, ring, mTest)
}
