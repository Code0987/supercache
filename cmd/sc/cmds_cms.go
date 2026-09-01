package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/Code0987/supercache/pkg/client"
)

// cmdCMSIncr is `cmsincr <name> <item> [n]`. n optional, default 1. 0 means 1.
func cmdCMSIncr(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 || len(args) > 3 {
		fmt.Fprintln(os.Stderr, "usage: cmsincr <name> <item> [n]")
		return 2
	}
	name := args[0]
	item := args[1]
	var n uint64 = 1
	if len(args) == 3 {
		parsed, err := strconv.ParseUint(args[2], 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "cmsincr: bad n %q\n", args[2])
			return 2
		}
		n = parsed
	}
	err := sess.withClient(func(cli *client.Client, _ string) error {
		return cli.CMSIncr(ctx, sess.cfg.keyspace, name, []byte(item), n)
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "cmsincr: %v\n", err)
		return 1
	}
	if !sess.cfg.quiet {
		fmt.Printf("OK cmsincr %s %s %d\n", name, item, n)
	}
	return 0
}

// cmdCMSQuery is `cmsquery <name> <item>`. Miss → "(nil)" + exit 1.
func cmdCMSQuery(ctx context.Context, sess *session, args []string) int {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: cmsquery <name> <item>")
		return 2
	}
	var n uint64
	var ok bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		n, ok, e = cli.CMSQuery(ctx, sess.cfg.keyspace, args[0], []byte(args[1]))
		return e
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "cmsquery: %v\n", err)
		return 1
	}
	if !ok {
		fmt.Println("(nil)")
		return 1
	}
	fmt.Println(n)
	return 0
}
