package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/engine"
)

func cmdXAdd(ctx context.Context, sess *session, args []string) int {
	if len(args) < 3 || args[1] != "*" {
		printUsageLine("usage: xadd <name> * <payload>")
		return 2
	}
	var id string
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		id, e = cli.XAdd(ctx, sess.cfg.keyspace, args[0], []byte(strings.Join(args[2:], " ")))
		return e
	})
	if err != nil {
		printCmdErr("xadd", err)
		return 1
	}
	if !sess.cfg.quiet {
		fmt.Println(id)
	}
	return 0
}

func cmdXLen(ctx context.Context, sess *session, args []string) int {
	if len(args) != 1 {
		printUsageLine("usage: xlen <name>")
		return 2
	}
	var n int
	var ok bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		n, ok, e = cli.XLen(ctx, sess.cfg.keyspace, args[0])
		return e
	})
	if err != nil {
		printCmdErr("xlen", err)
		return 1
	}
	if !ok {
		printNil()
		return 1
	}
	fmt.Println(n)
	return 0
}

func cmdXRange(ctx context.Context, sess *session, args []string) int {
	return cmdXRangeDir(ctx, sess, args, false)
}

func cmdXRevRange(ctx context.Context, sess *session, args []string) int {
	return cmdXRangeDir(ctx, sess, args, true)
}

func cmdXRangeDir(ctx context.Context, sess *session, args []string, rev bool) int {
	verb := "xrange"
	if rev {
		verb = "xrevrange"
	}
	if len(args) < 3 || len(args) > 4 {
		printUsageLine("usage: " + verb + " <name> <start> <end> [count]")
		return 2
	}
	count := 0
	if len(args) == 4 {
		n, err := strconv.Atoi(args[3])
		if err != nil {
			printUsageLine("usage: " + verb + " <name> <start> <end> [count]")
			return 2
		}
		count = n
	}
	var rows []engine.StreamEntry
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		if rev {
			rows, e = cli.XRevRange(ctx, sess.cfg.keyspace, args[0], args[1], args[2], count)
		} else {
			rows, e = cli.XRange(ctx, sess.cfg.keyspace, args[0], args[1], args[2], count)
		}
		return e
	})
	if err != nil {
		printCmdErr(verb, err)
		return 1
	}
	for _, r := range rows {
		fmt.Printf("%s %s\n", r.ID, r.Payload)
	}
	return 0
}

func cmdXDel(ctx context.Context, sess *session, args []string) int {
	if len(args) != 2 {
		printUsageLine("usage: xdel <name> <id>")
		return 2
	}
	err := sess.withClient(func(cli *client.Client, _ string) error {
		return cli.XDel(ctx, sess.cfg.keyspace, args[0], args[1])
	})
	if err != nil {
		printCmdErr("xdel", err)
		return 1
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("xdel %s %s", args[0], args[1]))
	}
	return 0
}

func cmdXTrim(ctx context.Context, sess *session, args []string) int {
	if len(args) != 2 {
		printUsageLine("usage: xtrim <name> <maxlen>")
		return 2
	}
	n, err := strconv.Atoi(args[1])
	if err != nil {
		printUsageLine("usage: xtrim <name> <maxlen>")
		return 2
	}
	err = sess.withClient(func(cli *client.Client, _ string) error {
		return cli.XTrim(ctx, sess.cfg.keyspace, args[0], n)
	})
	if err != nil {
		printCmdErr("xtrim", err)
		return 1
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("xtrim %s %s", args[0], args[1]))
	}
	return 0
}
