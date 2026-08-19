package store

import "testing"

func TestStoreGeoAddPosRemRadius(t *testing.T) {
	m := NewMemory(1 << 20)
	defer m.Close()
	if !m.GeoAdd("city", []byte("a"), -74.00, 40.70, 1, 0) {
		t.Fatal("add a")
	}
	if !m.GeoAdd("city", []byte("b"), -74.05, 40.70, 2, 0) {
		t.Fatal("add b")
	}
	if !m.GeoAdd("city", []byte("a"), -74.01, 40.71, 3, 0) {
		t.Fatal("update a")
	}
	lon, lat, ok := m.GeoPos("city", []byte("a"))
	if !ok || lon != -74.01 || lat != 40.71 {
		t.Fatalf("pos=%v %v %v", lon, lat, ok)
	}
	if m.GeoCard("city") != 2 {
		t.Fatal(m.GeoCard("city"))
	}
	r := m.GeoRadius("city", -74.01, 40.71, 20000, 0)
	if len(r) != 2 || string(r[0].Member) != "a" {
		t.Fatalf("%+v", r)
	}
	if !m.GeoRem("city", []byte("b"), 4, 0) {
		t.Fatal("rem")
	}
	if m.GeoCard("city") != 1 {
		t.Fatal(m.GeoCard("city"))
	}
}
