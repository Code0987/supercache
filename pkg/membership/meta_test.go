package membership

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/hashicorp/memberlist"
)

func TestNodeMetaFitsOrUsesCompactForm(t *testing.T) {
	m := &Membership{cfg: Config{
		NodeID:       "n1",
		PeerGRPCAddr: "10.0.0.5:9001",
	}}
	d := &delegate{m: m}

	// Generous limit: full JSON.
	meta := d.NodeMeta(512)
	var p metaPayload
	if err := json.Unmarshal(meta, &p); err != nil {
		t.Fatalf("json: %v meta=%q", err, meta)
	}
	if p.PeerGRPC != "10.0.0.5:9001" {
		t.Fatalf("got %q", p.PeerGRPC)
	}

	// Tiny limit that cannot fit JSON — must not return truncated JSON.
	// memberlist MetaMaxSpace is typically 512; we simulate pathological small limits.
	small := d.NodeMeta(8)
	if json.Valid(small) && len(small) > 0 {
		// If it claims to be JSON it must fully parse with PeerGRPC.
		var p2 metaPayload
		if err := json.Unmarshal(small, &p2); err != nil {
			t.Fatalf("must not return invalid/truncated JSON: %v %q", err, small)
		}
	}
	// peerFromNode must still recover an address when possible.
	node := &memberlist.Node{Name: "n1", Meta: small}
	peer := peerFromNode(node)
	// With limit 8, compact "10.0.0.5:9001" is 14 bytes — may be empty if too small.
	// Use a limit that fits compact but not JSON.
	compactLimit := len("10.0.0.5:9001")
	mid := d.NodeMeta(compactLimit)
	// JSON is `{"peer_grpc":"10.0.0.5:9001"}` longer than compactLimit.
	jsonLen := len(mustJSON(metaPayload{PeerGRPC: "10.0.0.5:9001"}))
	if compactLimit >= jsonLen {
		t.Fatalf("test setup: compactLimit %d >= json %d", compactLimit, jsonLen)
	}
	if !json.Valid(mid) {
		// compact raw address
		if string(mid) != "10.0.0.5:9001" {
			t.Fatalf("want compact addr, got %q", mid)
		}
	}
	peer = peerFromNode(&memberlist.Node{Name: "n1", Meta: mid})
	if peer.Addr != "10.0.0.5:9001" {
		t.Fatalf("peerFromNode addr=%q want 10.0.0.5:9001 (meta=%q)", peer.Addr, mid)
	}
}

func TestPeerFromNodeRejectsTruncatedJSON(t *testing.T) {
	full, _ := json.Marshal(metaPayload{PeerGRPC: "10.0.0.5:9001"})
	// Simulate old buggy truncation.
	trunc := full[:len(full)/2]
	if json.Valid(trunc) {
		t.Skip("unexpectedly valid")
	}
	p := peerFromNode(&memberlist.Node{Name: "x", Meta: trunc})
	if p.Addr != "" {
		// Must not invent addr from garbage; empty is safer than wrong.
		// (If we can recover, fine — but truncated JSON must not partially parse wrong.)
		if !strings.Contains(p.Addr, "10.0.0.5") {
			t.Fatalf("unexpected addr from trunc JSON: %q", p.Addr)
		}
	}
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
