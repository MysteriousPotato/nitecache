package hashring_test

import (
	"github.com/MysteriousPotato/nitecache/hashring"
	"github.com/MysteriousPotato/nitecache/test_utils"
	"testing"
)

func assertRing(t *testing.T, r *hashring.Ring, members []string) {
	ringMembers := r.Members()
	if !hashring.SliceEquals(members, ringMembers) {
		t.Errorf("expected: %v Members, got: %v Members", members, ringMembers)
	}

	gotPointsLen := len(r.Points())
	expectedPointsLen := len(ringMembers) * r.VirtualNodes()
	if gotPointsLen != expectedPointsLen {
		t.Errorf("expected: %v points, got: %v points", expectedPointsLen, gotPointsLen)
	}
}

func TestRing_New(t *testing.T) {
	mTest := []string{"node-1", "node-2", "node-3"}
	cfg := hashring.Opt{
		Members:      mTest,
		VirtualNodes: 10,
		HashFunc:     hashring.DefaultHashFunc,
	}

	ring, err := hashring.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	assertRing(t, ring, mTest)
}

func TestRing_SetMembers(t *testing.T) {
	mTest := []string{"node-1", "node-2", "node-3"}
	cfg := hashring.Opt{
		Members:      mTest,
		VirtualNodes: 10,
		HashFunc:     hashring.DefaultHashFunc,
	}

	ring, err := hashring.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	mTest[0] = "node-4"
	mTest = append(mTest, "node-5")
	if err = ring.SetMembers(mTest); err != nil {
		t.Fatal(err)
	}
	assertRing(t, ring, mTest)

	mTest = []string{"node-1", "node-2", "node-3"}

	if err = ring.SetMembers(mTest); err != nil {
		t.Fatal(err)
	}
	assertRing(t, ring, mTest)
}

func TestRing_GetOwner(t *testing.T) {
	mTest := []string{"10", "20", "30"}
	cfg := hashring.Opt{
		Members:      mTest,
		VirtualNodes: 10,
		HashFunc:     test.SimpleHashFunc,
	}

	ring, err := hashring.New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		expected string
		key      string
	}{{
		expected: "20",
		key:      "12",
	}, {
		expected: "10",
		key:      "0",
	}, {
		expected: "10",
		key:      "100",
	}, {
		expected: "30",
		key:      "30",
	}}

	for _, tt := range tests {
		owner, err := ring.GetOwner(tt.key)
		if err != nil {
			t.Fatal(err)
		}
		if owner != tt.expected {
			t.Fatalf("expected owner %s for key %s, got: %s", tt.expected, tt.key, owner)
		}
	}
}
