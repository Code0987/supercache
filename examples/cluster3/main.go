// Example: exercise a 3-node SuperCache cluster (CacheOnly + ModeSet).
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Code0987/supercache/pkg/client"
)

func main() {
	ctx := context.Background()
	addrs := []string{"127.0.0.1:9010", "127.0.0.1:9011", "127.0.0.1:9012"}
	clients := make([]*client.Client, 0, 3)
	for _, a := range addrs {
		c, err := client.Dial(ctx, a)
		if err != nil {
			fmt.Fprintf(os.Stderr, "dial %s: %v\n", a, err)
			os.Exit(1)
		}
		defer c.Close()
		clients = append(clients, c)
	}

	fmt.Println("=== 1) Put session:user1 via n1 (CacheOnly demo) ===")
	if err := clients[0].Put(ctx, "demo", "session:user1",
		[]byte(`{"user":"alice","role":"admin"}`), client.WithTTL(2*time.Minute)); err != nil {
		fmt.Println("put err:", err)
		os.Exit(1)
	}
	fmt.Println("    Put OK on n1")
	time.Sleep(500 * time.Millisecond)

	fmt.Println("\n=== 2) Get session:user1 from all 3 nodes (fan-out check) ===")
	for i, c := range clients {
		v, err := c.Get(ctx, "demo", "session:user1")
		if err != nil {
			fmt.Printf("    n%d ERROR: %v\n", i+1, err)
		} else {
			fmt.Printf("    n%d OK: %s\n", i+1, string(v))
		}
	}

	fmt.Println("\n=== 3) ModeSet feature flags via n2 (SetAdd into set name features) ===")
	// Exact membership: one set name, many items — not one KV key per flag.
	for _, item := range []string{"dark_mode", "beta", "max_cart"} {
		if err := clients[1].SetAdd(ctx, "tags", "features", []byte(item)); err != nil {
			fmt.Printf("    SetAdd %s err: %v\n", item, err)
			os.Exit(1)
		}
	}
	fmt.Println("    SetAdd dark_mode, beta, max_cart OK on n2")
	time.Sleep(500 * time.Millisecond)

	fmt.Println("\n=== 4) SetContains / SetCard / SetMembers from n3 ===")
	for _, item := range []string{"dark_mode", "beta", "max_cart", "unknown_flag"} {
		ok, err := clients[2].SetContains(ctx, "tags", "features", []byte(item))
		if err != nil {
			fmt.Printf("    contains %s ERROR: %v\n", item, err)
		} else {
			fmt.Printf("    contains %s = %v\n", item, ok)
		}
	}
	n, err := clients[2].SetCard(ctx, "tags", "features")
	if err != nil {
		fmt.Println("    card ERROR:", err)
	} else {
		fmt.Printf("    SetCard(features) = %d\n", n)
	}
	mem, err := clients[2].SetMembers(ctx, "tags", "features")
	if err != nil {
		fmt.Println("    members ERROR:", err)
	} else {
		fmt.Printf("    SetMembers =")
		for _, m := range mem {
			fmt.Printf(" %s", m)
		}
		fmt.Println()
	}

	fmt.Println("\n=== 5) SetRemove beta via n1; confirm on n2 ===")
	if err := clients[0].SetRemove(ctx, "tags", "features", []byte("beta")); err != nil {
		fmt.Println("    remove err:", err)
	} else {
		fmt.Println("    SetRemove beta OK")
	}
	time.Sleep(400 * time.Millisecond)
	ok, err := clients[1].SetContains(ctx, "tags", "features", []byte("beta"))
	fmt.Printf("    n2 contains beta = %v err=%v (expect false)\n", ok, err)

	fmt.Println("\n=== 6) Delete session:user1 via n3 ===")
	if err := clients[2].Delete(ctx, "demo", "session:user1"); err != nil {
		fmt.Println("    delete note:", err)
	} else {
		fmt.Println("    Delete OK (all peers ACKed)")
	}
	time.Sleep(300 * time.Millisecond)

	fmt.Println("\n=== 7) Confirm session miss on all nodes ===")
	for i, c := range clients {
		_, err := c.Get(ctx, "demo", "session:user1")
		fmt.Printf("    n%d: %v\n", i+1, err)
	}

	fmt.Println("\n=== 8) Spread 12 KV keys (write rotate, read next node) ===")
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("item:%d", i)
		_ = clients[i%3].Put(ctx, "demo", k, []byte(fmt.Sprintf("value-%d", i)))
	}
	time.Sleep(600 * time.Millisecond)
	hits := 0
	for i := 0; i < 12; i++ {
		k := fmt.Sprintf("item:%d", i)
		want := fmt.Sprintf("value-%d", i)
		c := clients[(i+1)%3]
		if v, err := c.Get(ctx, "demo", k); err == nil && string(v) == want {
			hits++
		} else {
			fmt.Printf("    lag/miss %s: %v\n", k, err)
		}
	}
	fmt.Printf("    Cross-node consistent reads: %d/12\n", hits)

	fmt.Println("\n=== Done ===")
}
