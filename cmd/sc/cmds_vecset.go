package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/Code0987/supercache/pkg/client"
	"github.com/Code0987/supercache/pkg/engine"
)

func parseVec(s string) ([]float32, error) {
	parts := strings.Split(s, ",")
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty vector")
	}
	out := make([]float32, len(parts))
	for i, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 32)
		if err != nil {
			return nil, err
		}
		out[i] = float32(f)
	}
	return out, nil
}

func cmdVAdd(ctx context.Context, sess *session, args []string) int {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: vadd <name> <member> <f1,f2,…>")
		return 2
	}
	vec, err := parseVec(args[2])
	if err != nil {
		fmt.Fprintf(os.Stderr, "vadd: bad vec %q\n", args[2])
		return 2
	}
	err = sess.withClient(func(cli *client.Client, _ string) error {
		return cli.VAdd(ctx, sess.cfg.keyspace, args[0], []byte(args[1]), vec)
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "vadd: %v\n", err)
		return 1
	}
	if !sess.cfg.quiet {
		fmt.Printf("OK vadd %s %s\n", args[0], args[1])
	}
	return 0
}

func cmdVRem(ctx context.Context, sess *session, args []string) int {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: vrem <name> <member>")
		return 2
	}
	err := sess.withClient(func(cli *client.Client, _ string) error {
		return cli.VRem(ctx, sess.cfg.keyspace, args[0], []byte(args[1]))
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "vrem: %v\n", err)
		return 1
	}
	if !sess.cfg.quiet {
		fmt.Printf("OK vrem %s %s\n", args[0], args[1])
	}
	return 0
}

func cmdVSim(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 || len(args) > 3 {
		fmt.Fprintln(os.Stderr, "usage: vsim <name> <f1,f2,…> [k]")
		return 2
	}
	vec, err := parseVec(args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "vsim: bad vec %q\n", args[1])
		return 2
	}
	k := 0
	if len(args) == 3 {
		parsed, err := strconv.Atoi(args[2])
		if err != nil {
			fmt.Fprintf(os.Stderr, "vsim: bad k %q\n", args[2])
			return 2
		}
		k = parsed
	}
	var hits []engine.VSimHit
	err = sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		hits, e = cli.VSim(ctx, sess.cfg.keyspace, args[0], vec, k)
		return e
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "vsim: %v\n", err)
		return 1
	}
	for _, h := range hits {
		fmt.Printf("%g %s\n", h.Score, h.Member)
	}
	return 0
}

func cmdVCard(ctx context.Context, sess *session, args []string) int {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: vcard <name>")
		return 2
	}
	var n int
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		n, _, e = cli.VCard(ctx, sess.cfg.keyspace, args[0])
		return e
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "vcard: %v\n", err)
		return 1
	}
	fmt.Println(n)
	return 0
}

func cmdVDim(ctx context.Context, sess *session, args []string) int {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "usage: vdim <name>")
		return 2
	}
	var dim int
	var ok bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		dim, ok, e = cli.VDim(ctx, sess.cfg.keyspace, args[0])
		return e
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "vdim: %v\n", err)
		return 1
	}
	if !ok {
		fmt.Println("missing")
		return 1
	}
	fmt.Println(dim)
	return 0
}

func cmdVEmb(ctx context.Context, sess *session, args []string) int {
	if len(args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: vemb <name> <member>")
		return 2
	}
	var vec []float32
	var ok bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		vec, ok, e = cli.VEmb(ctx, sess.cfg.keyspace, args[0], []byte(args[1]))
		return e
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "vemb: %v\n", err)
		return 1
	}
	if !ok {
		fmt.Println("missing")
		return 1
	}
	parts := make([]string, len(vec))
	for i, x := range vec {
		parts[i] = strconv.FormatFloat(float64(x), 'g', -1, 32)
	}
	fmt.Println(strings.Join(parts, ","))
	return 0
}
