package membership

import (
	"testing"

	"github.com/hashicorp/memberlist"
)

func TestPreferLocalGossipForLoopback(t *testing.T) {
	if !preferLocalGossip(Config{AdvertiseAddr: "127.0.0.1", BindAddr: "0.0.0.0"}) {
		t.Fatal("loopback advertise should use local gossip timings")
	}
	if !preferLocalGossip(Config{BindAddr: "127.0.0.1"}) {
		t.Fatal("loopback bind should use local gossip timings")
	}
	if preferLocalGossip(Config{AdvertiseAddr: "10.0.0.5", BindAddr: "0.0.0.0"}) {
		t.Fatal("non-loopback should use LAN gossip timings")
	}
}

func TestMemberlistBaseConfigLANVsLocal(t *testing.T) {
	local := baseMemberlistConfig(Config{
		NodeID: "n1", PeerGRPCAddr: "127.0.0.1:9001",
		BindAddr: "127.0.0.1", AdvertiseAddr: "127.0.0.1", BindPort: 0,
	})
	lan := baseMemberlistConfig(Config{
		NodeID: "n1", PeerGRPCAddr: "10.0.0.1:9001",
		BindAddr: "0.0.0.0", AdvertiseAddr: "10.0.0.1", BindPort: 0,
	})
	// DefaultLocalConfig and DefaultLANConfig differ on suspicion/probe settings.
	if local.ProbeInterval == lan.ProbeInterval && local.SuspicionMult == lan.SuspicionMult {
		// Still assert we actually picked the stock constructors.
		refLocal := memberlist.DefaultLocalConfig()
		refLAN := memberlist.DefaultLANConfig()
		if local.ProbeInterval != refLocal.ProbeInterval {
			t.Fatalf("loopback config ProbeInterval=%v want local %v", local.ProbeInterval, refLocal.ProbeInterval)
		}
		if lan.ProbeInterval != refLAN.ProbeInterval {
			t.Fatalf("LAN config ProbeInterval=%v want lan %v", lan.ProbeInterval, refLAN.ProbeInterval)
		}
	}
	refLocal := memberlist.DefaultLocalConfig()
	refLAN := memberlist.DefaultLANConfig()
	if local.ProbeInterval != refLocal.ProbeInterval {
		t.Fatalf("loopback: got ProbeInterval=%v want %v (DefaultLocalConfig)", local.ProbeInterval, refLocal.ProbeInterval)
	}
	if lan.ProbeInterval != refLAN.ProbeInterval {
		t.Fatalf("non-loopback: got ProbeInterval=%v want %v (DefaultLANConfig)", lan.ProbeInterval, refLAN.ProbeInterval)
	}
}
