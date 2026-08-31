package main

import (
	"context"
	"fmt"
	"os"

	"github.com/Code0987/supercache/pkg/client"
)

// cmdTopKAdd is `topkadd <name> <item...>` — one TopKAdd RPC per item.
func cmdTopKAdd(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: topkadd <name> <item...>")
		return 2
	}
	name := args[0]
	for _, item := range args[1:] {
		err := sess.withClient(func(cli *client.Client, _ string) error {
			return cli.TopKAdd(ctx, sess.cfg.keyspace, name, []byte(item))
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "topkadd: %v\n", err)
			return 1
		}
		if !sess.cfg.quiet {
			fmt.Printf("OK topkadd %s %s\n", name, item)
		}
	}
	return 0
}

// cmdTopKList is `topklist <name>`. Miss → "(nil)" + exit 1. Live empty chart → exit 0, no lines.
func cmdTopKList(ctx context.Context, sess *session, args []string) int {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: topklist <name>")
		return 2
	}
	var rows []client.TopKEntry
	var ok bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		rows, ok, e = cli.TopKList(ctx, sess.cfg.keyspace, args[0])
		return e
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "topklist: %v\n", err)
		return 1
	}
	if !ok {
		fmt.Println("(nil)")
		return 1
	}
	for _, r := range rows {
		fmt.Printf("%s %d\n", r.Item, r.Count)
	}
	return 0
}
