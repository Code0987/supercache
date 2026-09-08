package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/Code0987/supercache/pkg/client"
)

func dispatch(ctx context.Context, sess *session, cmd string, args []string) int {
	switch strings.ToLower(cmd) {
	case "get":
		return cmdGet(ctx, sess, args)
	case "put", "set":
		return cmdPut(ctx, sess, args)
	case "del", "delete", "rm":
		return cmdDel(ctx, sess, args)
	case "ping":
		return cmdPing(ctx, sess)
	case "peers":
		return cmdAdmin(ctx, sess.cfg, "/peers")
	case "keyspaces", "ks":
		return cmdAdmin(ctx, sess.cfg, "/keyspaces")
	case "metrics":
		return cmdAdmin(ctx, sess.cfg, "/metrics")
	case "health", "healthz":
		return cmdAdmin(ctx, sess.cfg, "/healthz")
	case "ready", "readyz":
		return cmdAdmin(ctx, sess.cfg, "/readyz")
	case "bloom":
		return cmdBloom(ctx, sess, args)
	case "sadd":
		return cmdSAdd(ctx, sess, args)
	case "srem":
		return cmdSRem(ctx, sess, args)
	case "sismember":
		return cmdSIsMember(ctx, sess, args)
	case "scard":
		return cmdSCard(ctx, sess, args)
	case "smembers":
		return cmdSMembers(ctx, sess, args)
	case "zadd":
		return cmdZAdd(ctx, sess, args)
	case "zrem":
		return cmdZRem(ctx, sess, args)
	case "zscore":
		return cmdZScore(ctx, sess, args)
	case "zcard":
		return cmdZCard(ctx, sess, args)
	case "zrange":
		return cmdZRange(ctx, sess, args)
	case "zrangebyscore":
		return cmdZRangeByScore(ctx, sess, args)
	case "geoadd":
		return cmdGeoAdd(ctx, sess, args)
	case "georem":
		return cmdGeoRem(ctx, sess, args)
	case "geopos":
		return cmdGeoPos(ctx, sess, args)
	case "geocard":
		return cmdGeoCard(ctx, sess, args)
	case "geodist":
		return cmdGeoDist(ctx, sess, args)
	case "georadius":
		return cmdGeoRadius(ctx, sess, args)
	case "lpush":
		return cmdLPush(ctx, sess, args)
	case "rpush":
		return cmdRPush(ctx, sess, args)
	case "lpop":
		return cmdLPop(ctx, sess, args)
	case "rpop":
		return cmdRPop(ctx, sess, args)
	case "llen":
		return cmdLLen(ctx, sess, args)
	case "lindex":
		return cmdLIndex(ctx, sess, args)
	case "lrange":
		return cmdLRange(ctx, sess, args)
	case "hset":
		return cmdHSet(ctx, sess, args)
	case "hget":
		return cmdHGet(ctx, sess, args)
	case "hdel":
		return cmdHDel(ctx, sess, args)
	case "hexists":
		return cmdHExists(ctx, sess, args)
	case "hlen":
		return cmdHLen(ctx, sess, args)
	case "hgetall":
		return cmdHGetAll(ctx, sess, args)
	case "incr":
		return cmdIncr(ctx, sess, args)
	case "cget":
		return cmdCGet(ctx, sess, args)
	case "jsonset":
		return cmdJSONSet(ctx, sess, args)
	case "jsonget":
		return cmdJSONGet(ctx, sess, args)
	case "jsondel":
		return cmdJSONDel(ctx, sess, args)
	case "bitset":
		return cmdBitSet(ctx, sess, args)
	case "bitget":
		return cmdBitGet(ctx, sess, args)
	case "bitcount":
		return cmdBitCount(ctx, sess, args)
	case "bitpos":
		return cmdBitPos(ctx, sess, args)
	case "hlladd":
		return cmdHLLAdd(ctx, sess, args)
	case "hllcount":
		return cmdHLLCount(ctx, sess, args)
	case "topkadd":
		return cmdTopKAdd(ctx, sess, args)
	case "topklist":
		return cmdTopKList(ctx, sess, args)
	case "cmsincr":
		return cmdCMSIncr(ctx, sess, args)
	case "cmsquery":
		return cmdCMSQuery(ctx, sess, args)
	case "vadd":
		return cmdVAdd(ctx, sess, args)
	case "vrem":
		return cmdVRem(ctx, sess, args)
	case "vsim":
		return cmdVSim(ctx, sess, args)
	case "vcard":
		return cmdVCard(ctx, sess, args)
	case "vdim":
		return cmdVDim(ctx, sess, args)
	case "xadd":
		return cmdXAdd(ctx, sess, args)
	case "xlen":
		return cmdXLen(ctx, sess, args)
	case "xrange":
		return cmdXRange(ctx, sess, args)
	case "xrevrange":
		return cmdXRevRange(ctx, sess, args)
	case "xdel":
		return cmdXDel(ctx, sess, args)
	case "xtrim":
		return cmdXTrim(ctx, sess, args)
	case "vemb":
		return cmdVEmb(ctx, sess, args)
	default:
		printUnknown(cmd)
		printNote("type help for commands")
		return 2
	}
}

func cmdGet(ctx context.Context, sess *session, keys []string) int {
	if len(keys) == 0 {
		printUsageLine("usage: get <key> [key...]")
		return 2
	}
	cfg := sess.cfg

	type item struct {
		Key    string `json:"key"`
		Found  bool   `json:"found"`
		Value  string `json:"value,omitempty"`
		Base64 bool   `json:"base64,omitempty"`
		Error  string `json:"error,omitempty"`
	}
	var items []item
	missing := 0

	err := sess.withClient(func(cli *client.Client, _ string) error {
		items = items[:0]
		missing = 0
		for _, k := range keys {
			v, err := cli.Get(ctx, cfg.keyspace, k)
			it := item{Key: k}
			switch {
			case errors.Is(err, client.ErrNotFound):
				it.Found = false
				missing++
			case err != nil:
				// transport? bubble for failover
				if isDialRetryable(err) {
					return err
				}
				it.Error = err.Error()
				missing++
			default:
				it.Found = true
				if cfg.base64 {
					it.Value = base64.StdEncoding.EncodeToString(v)
					it.Base64 = true
				} else if utf8.Valid(v) && isMostlyPrintable(v) {
					it.Value = string(v)
				} else {
					it.Value = base64.StdEncoding.EncodeToString(v)
					it.Base64 = true
				}
			}
			items = append(items, it)
		}
		return nil
	})
	if err != nil {
		printCmdErr("get", err)
		return 1
	}

	if cfg.jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(map[string]any{
			"keyspace": cfg.keyspace,
			"seed":     sess.ConnectedAddr(),
			"items":    items,
		})
		if missing > 0 {
			return 1
		}
		return 0
	}

	for _, it := range items {
		if it.Error != "" {
			printCmdMsg(it.Key, it.Error)
			continue
		}
		if !it.Found {
			if len(keys) == 1 {
				printNil()
			} else {
				fmt.Printf("%s\t%s\n", it.Key, paint(os.Stdout, ansiDim, "(nil)"))
			}
			continue
		}
		if len(keys) == 1 {
			if cfg.raw {
				_, _ = os.Stdout.WriteString(it.Value)
			} else {
				fmt.Println(it.Value)
				if it.Base64 && !cfg.base64 {
					printNote("binary value shown as base64; pass -base64 to make it explicit")
				}
			}
			continue
		}
		if it.Base64 {
			fmt.Printf("%s\t%s\t%s\n", it.Key, paint(os.Stdout, ansiDim, "(base64)"), it.Value)
		} else {
			fmt.Printf("%s\t%s\n", it.Key, it.Value)
		}
	}
	if missing > 0 {
		return 1
	}
	return 0
}

func cmdPut(ctx context.Context, sess *session, args []string) int {
	if len(args) < 1 {
		printUsageLine("usage: put <key> <value> | put <key> -file <path>")
		return 2
	}
	cfg := sess.cfg
	key := args[0]
	var value []byte
	var err error

	switch {
	case cfg.filePath != "":
		value, err = readFileOrStdin(cfg.filePath)
		if err != nil {
			printCmdErr("put", err)
			return 1
		}
	case len(args) >= 2:
		raw := strings.Join(args[1:], " ")
		if cfg.base64 {
			value, err = base64.StdEncoding.DecodeString(raw)
			if err != nil {
				printCmdMsg("put", fmt.Sprintf("bad base64: %v", err))
				return 1
			}
		} else {
			value = []byte(raw)
		}
	default:
		printUsageLine("usage: put <key> <value> | put <key> -file <path>")
		return 2
	}

	var opts []client.PutOption
	if cfg.ttlSet {
		opts = append(opts, client.WithTTL(cfg.ttl))
	}

	err = sess.withClient(func(cli *client.Client, _ string) error {
		return cli.Put(ctx, cfg.keyspace, key, value, opts...)
	})
	if err != nil {
		printCmdErr("put", err)
		return 1
	}
	if cfg.jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"ok": true, "keyspace": cfg.keyspace, "key": key,
			"bytes": len(value), "ttl_set": cfg.ttlSet, "ttl": cfg.ttl.String(),
			"seed": sess.ConnectedAddr(),
		})
	} else if !cfg.quiet {
		printOK(fmt.Sprintf("put %s (%d bytes)", key, len(value)))
	}
	return 0
}

func cmdDel(ctx context.Context, sess *session, keys []string) int {
	if len(keys) == 0 {
		printUsageLine("usage: del <key> [key...]")
		return 2
	}
	cfg := sess.cfg

	var delErr error
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		if len(keys) == 1 {
			e = cli.Delete(ctx, cfg.keyspace, keys[0])
		} else {
			e = cli.DeleteMany(ctx, cfg.keyspace, keys)
		}
		// Peer partial failures are not transport failures.
		var pf client.PeerFailures
		var ke client.KeyErrors
		if e != nil && (errors.As(e, &pf) || errors.As(e, &ke)) {
			delErr = e
			return nil
		}
		if e != nil && isDialRetryable(e) {
			return e
		}
		delErr = e
		return nil
	})
	if err != nil {
		printCmdErr("del", err)
		return 1
	}
	if delErr != nil {
		var pf client.PeerFailures
		var ke client.KeyErrors
		if errors.As(delErr, &pf) || errors.As(delErr, &ke) {
			printWarn(delErr.Error())
			if cfg.jsonOut {
				_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
					"ok": true, "partial": true, "keyspace": cfg.keyspace,
					"keys": keys, "warning": delErr.Error(), "seed": sess.ConnectedAddr(),
				})
			} else if !cfg.quiet {
				printOK("del " + strings.Join(keys, " "))
			}
			return 0
		}
		printCmdErr("del", delErr)
		return 1
	}
	if cfg.jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
			"ok": true, "keyspace": cfg.keyspace, "keys": keys, "seed": sess.ConnectedAddr(),
		})
	} else if !cfg.quiet {
		printOK("del " + strings.Join(keys, " "))
	}
	return 0
}

func cmdBloom(ctx context.Context, sess *session, args []string) int {
	if len(args) < 3 {
		printUsageLine("usage: bloom add|test <name> <item>")
		return 2
	}
	op, name, item := strings.ToLower(args[0]), args[1], args[2]
	if op != "add" && op != "test" {
		printUsageLine("usage: bloom add|test <name> <item>")
		return 2
	}
	var (
		maybe bool
		opErr error
	)
	err := sess.withClient(func(cli *client.Client, _ string) error {
		switch op {
		case "add":
			opErr = cli.BloomAdd(ctx, sess.cfg.keyspace, name, []byte(item))
			return opErr
		default:
			maybe, opErr = cli.BloomTest(ctx, sess.cfg.keyspace, name, []byte(item))
			return opErr
		}
	})
	if err != nil {
		printCmdErr("bloom", err)
		return 1
	}
	if opErr != nil {
		printCmdErr("bloom", opErr)
		return 1
	}
	if op == "test" {
		printBool(maybe)
		return 0
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("bloom add %s %s", name, item))
	}
	return 0
}

func cmdSAdd(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 {
		printUsageLine("usage: sadd <name> <item>")
		return 2
	}
	name, item := args[0], args[1]
	err := sess.withClient(func(cli *client.Client, _ string) error {
		return cli.SetAdd(ctx, sess.cfg.keyspace, name, []byte(item))
	})
	if err != nil {
		printCmdErr("sadd", err)
		return 1
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("sadd %s %s", name, item))
	}
	return 0
}

func cmdSRem(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 {
		printUsageLine("usage: srem <name> <item>")
		return 2
	}
	name, item := args[0], args[1]
	err := sess.withClient(func(cli *client.Client, _ string) error {
		return cli.SetRemove(ctx, sess.cfg.keyspace, name, []byte(item))
	})
	if err != nil {
		printCmdErr("srem", err)
		return 1
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("srem %s %s", name, item))
	}
	return 0
}

func cmdSIsMember(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 {
		printUsageLine("usage: sismember <name> <item>")
		return 2
	}
	name, item := args[0], args[1]
	var present bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		present, e = cli.SetContains(ctx, sess.cfg.keyspace, name, []byte(item))
		return e
	})
	if err != nil {
		printCmdErr("sismember", err)
		return 1
	}
	printBool(present)
	if !present {
		return 1
	}
	return 0
}

func cmdSCard(ctx context.Context, sess *session, args []string) int {
	if len(args) < 1 {
		printUsageLine("usage: scard <name>")
		return 2
	}
	name := args[0]
	var n int
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		n, e = cli.SetCard(ctx, sess.cfg.keyspace, name)
		return e
	})
	if err != nil {
		printCmdErr("scard", err)
		return 1
	}
	fmt.Println(n)
	return 0
}

func cmdSMembers(ctx context.Context, sess *session, args []string) int {
	if len(args) < 1 {
		printUsageLine("usage: smembers <name>")
		return 2
	}
	name := args[0]
	var mem [][]byte
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		mem, e = cli.SetMembers(ctx, sess.cfg.keyspace, name)
		return e
	})
	if err != nil {
		printCmdErr("smembers", err)
		return 1
	}
	for _, m := range mem {
		fmt.Println(string(m))
	}
	return 0
}

func cmdZAdd(ctx context.Context, sess *session, args []string) int {
	if len(args) < 3 {
		printUsageLine("usage: zadd <name> <score> <member>")
		return 2
	}
	name, scoreStr, member := args[0], args[1], args[2]
	score, err := parseFloat(scoreStr)
	if err != nil {
		printCmdMsg("zadd", fmt.Sprintf("bad score %q: %v", scoreStr, err))
		return 2
	}
	err = sess.withClient(func(cli *client.Client, _ string) error {
		return cli.ZAdd(ctx, sess.cfg.keyspace, name, []byte(member), score)
	})
	if err != nil {
		printCmdErr("zadd", err)
		return 1
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("zadd %s %g %s", name, score, member))
	}
	return 0
}

func cmdZRem(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 {
		printUsageLine("usage: zrem <name> <member>")
		return 2
	}
	name, member := args[0], args[1]
	err := sess.withClient(func(cli *client.Client, _ string) error {
		return cli.ZRem(ctx, sess.cfg.keyspace, name, []byte(member))
	})
	if err != nil {
		printCmdErr("zrem", err)
		return 1
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("zrem %s %s", name, member))
	}
	return 0
}

func cmdZScore(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 {
		printUsageLine("usage: zscore <name> <member>")
		return 2
	}
	name, member := args[0], args[1]
	var (
		score float64
		ok    bool
	)
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		score, ok, e = cli.ZScore(ctx, sess.cfg.keyspace, name, []byte(member))
		return e
	})
	if err != nil {
		printCmdErr("zscore", err)
		return 1
	}
	if !ok {
		printNil()
		return 1
	}
	fmt.Println(score)
	return 0
}

func cmdZCard(ctx context.Context, sess *session, args []string) int {
	if len(args) < 1 {
		printUsageLine("usage: zcard <name>")
		return 2
	}
	name := args[0]
	var n int
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		n, e = cli.ZCard(ctx, sess.cfg.keyspace, name)
		return e
	})
	if err != nil {
		printCmdErr("zcard", err)
		return 1
	}
	fmt.Println(n)
	return 0
}

func cmdZRange(ctx context.Context, sess *session, args []string) int {
	if len(args) < 3 {
		printUsageLine("usage: zrange <name> <start> <stop>")
		return 2
	}
	name := args[0]
	start, err := parseInt(args[1])
	if err != nil {
		printCmdMsg("zrange", fmt.Sprintf("bad start %q: %v", args[1], err))
		return 2
	}
	stop, err := parseInt(args[2])
	if err != nil {
		printCmdMsg("zrange", fmt.Sprintf("bad stop %q: %v", args[2], err))
		return 2
	}
	var mem []client.ZMember
	err = sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		mem, e = cli.ZRange(ctx, sess.cfg.keyspace, name, start, stop)
		return e
	})
	if err != nil {
		printCmdErr("zrange", err)
		return 1
	}
	printZMembers(mem)
	return 0
}

func cmdZRangeByScore(ctx context.Context, sess *session, args []string) int {
	if len(args) < 3 {
		printUsageLine("usage: zrangebyscore <name> <min> <max>")
		return 2
	}
	name := args[0]
	min, err := parseFloat(args[1])
	if err != nil {
		printCmdMsg("zrangebyscore", fmt.Sprintf("bad min %q: %v", args[1], err))
		return 2
	}
	max, err := parseFloat(args[2])
	if err != nil {
		printCmdMsg("zrangebyscore", fmt.Sprintf("bad max %q: %v", args[2], err))
		return 2
	}
	var mem []client.ZMember
	err = sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		mem, e = cli.ZRangeByScore(ctx, sess.cfg.keyspace, name, min, max)
		return e
	})
	if err != nil {
		printCmdErr("zrangebyscore", err)
		return 1
	}
	printZMembers(mem)
	return 0
}

func printZMembers(mem []client.ZMember) {
	for _, m := range mem {
		fmt.Printf("%g %s\n", m.Score, string(m.Member))
	}
}

func cmdGeoAdd(ctx context.Context, sess *session, args []string) int {
	if len(args) < 4 {
		printUsageLine("usage: geoadd <name> <lon> <lat> <member>")
		return 2
	}
	name, member := args[0], args[3]
	lon, err := parseFloat(args[1])
	if err != nil {
		printCmdMsg("geoadd", fmt.Sprintf("bad lon %q: %v", args[1], err))
		return 2
	}
	lat, err := parseFloat(args[2])
	if err != nil {
		printCmdMsg("geoadd", fmt.Sprintf("bad lat %q: %v", args[2], err))
		return 2
	}
	err = sess.withClient(func(cli *client.Client, _ string) error {
		return cli.GeoAdd(ctx, sess.cfg.keyspace, name, []byte(member), lon, lat)
	})
	if err != nil {
		printCmdErr("geoadd", err)
		return 1
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("geoadd %s %g %g %s", name, lon, lat, member))
	}
	return 0
}

func cmdGeoRem(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 {
		printUsageLine("usage: georem <name> <member>")
		return 2
	}
	name, member := args[0], args[1]
	err := sess.withClient(func(cli *client.Client, _ string) error {
		return cli.GeoRem(ctx, sess.cfg.keyspace, name, []byte(member))
	})
	if err != nil {
		printCmdErr("georem", err)
		return 1
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("georem %s %s", name, member))
	}
	return 0
}

func cmdGeoPos(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 {
		printUsageLine("usage: geopos <name> <member>")
		return 2
	}
	name, member := args[0], args[1]
	var lon, lat float64
	var ok bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		lon, lat, ok, e = cli.GeoPos(ctx, sess.cfg.keyspace, name, []byte(member))
		return e
	})
	if err != nil {
		printCmdErr("geopos", err)
		return 1
	}
	if !ok {
		printNil()
		return 1
	}
	fmt.Printf("%g %g\n", lon, lat)
	return 0
}

func cmdGeoCard(ctx context.Context, sess *session, args []string) int {
	if len(args) < 1 {
		printUsageLine("usage: geocard <name>")
		return 2
	}
	var n int
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		n, e = cli.GeoCard(ctx, sess.cfg.keyspace, args[0])
		return e
	})
	if err != nil {
		printCmdErr("geocard", err)
		return 1
	}
	fmt.Println(n)
	return 0
}

func cmdGeoDist(ctx context.Context, sess *session, args []string) int {
	if len(args) < 3 {
		printUsageLine("usage: geodist <name> <a> <b>")
		return 2
	}
	var meters float64
	var ok bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		meters, ok, e = cli.GeoDist(ctx, sess.cfg.keyspace, args[0], []byte(args[1]), []byte(args[2]))
		return e
	})
	if err != nil {
		printCmdErr("geodist", err)
		return 1
	}
	if !ok {
		printNil()
		return 1
	}
	fmt.Println(meters)
	return 0
}

func cmdGeoRadius(ctx context.Context, sess *session, args []string) int {
	if len(args) < 4 {
		printUsageLine("usage: georadius <name> <lon> <lat> <radius_m> [limit]")
		return 2
	}
	lon, err := parseFloat(args[1])
	if err != nil {
		printCmdMsg("georadius", fmt.Sprintf("bad lon %q: %v", args[1], err))
		return 2
	}
	lat, err := parseFloat(args[2])
	if err != nil {
		printCmdMsg("georadius", fmt.Sprintf("bad lat %q: %v", args[2], err))
		return 2
	}
	rad, err := parseFloat(args[3])
	if err != nil {
		printCmdMsg("georadius", fmt.Sprintf("bad radius %q: %v", args[3], err))
		return 2
	}
	limit := 0
	if len(args) >= 5 {
		limit, err = parseInt(args[4])
		if err != nil {
			printCmdMsg("georadius", fmt.Sprintf("bad limit %q: %v", args[4], err))
			return 2
		}
	}
	var mem []client.GeoMember
	err = sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		mem, e = cli.GeoRadius(ctx, sess.cfg.keyspace, args[0], lon, lat, rad, limit)
		return e
	})
	if err != nil {
		printCmdErr("georadius", err)
		return 1
	}
	for _, m := range mem {
		fmt.Printf("%g %s %g %g\n", m.Dist, string(m.Member), m.Lon, m.Lat)
	}
	return 0
}

func cmdLPush(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 {
		printUsageLine("usage: lpush <name> <item>")
		return 2
	}
	err := sess.withClient(func(cli *client.Client, _ string) error {
		return cli.LPush(ctx, sess.cfg.keyspace, args[0], []byte(args[1]))
	})
	if err != nil {
		printCmdErr("lpush", err)
		return 1
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("lpush %s %s", args[0], args[1]))
	}
	return 0
}

func cmdRPush(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 {
		printUsageLine("usage: rpush <name> <item>")
		return 2
	}
	err := sess.withClient(func(cli *client.Client, _ string) error {
		return cli.RPush(ctx, sess.cfg.keyspace, args[0], []byte(args[1]))
	})
	if err != nil {
		printCmdErr("rpush", err)
		return 1
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("rpush %s %s", args[0], args[1]))
	}
	return 0
}

func cmdLPop(ctx context.Context, sess *session, args []string) int {
	if len(args) < 1 {
		printUsageLine("usage: lpop <name>")
		return 2
	}
	var item []byte
	var ok bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		item, ok, e = cli.LPop(ctx, sess.cfg.keyspace, args[0])
		return e
	})
	if err != nil {
		printCmdErr("lpop", err)
		return 1
	}
	if !ok {
		printNil()
		return 1
	}
	fmt.Println(string(item))
	return 0
}

func cmdRPop(ctx context.Context, sess *session, args []string) int {
	if len(args) < 1 {
		printUsageLine("usage: rpop <name>")
		return 2
	}
	var item []byte
	var ok bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		item, ok, e = cli.RPop(ctx, sess.cfg.keyspace, args[0])
		return e
	})
	if err != nil {
		printCmdErr("rpop", err)
		return 1
	}
	if !ok {
		printNil()
		return 1
	}
	fmt.Println(string(item))
	return 0
}

func cmdLLen(ctx context.Context, sess *session, args []string) int {
	if len(args) < 1 {
		printUsageLine("usage: llen <name>")
		return 2
	}
	var n int
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		n, e = cli.LLen(ctx, sess.cfg.keyspace, args[0])
		return e
	})
	if err != nil {
		printCmdErr("llen", err)
		return 1
	}
	fmt.Println(n)
	return 0
}

func cmdLIndex(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 {
		printUsageLine("usage: lindex <name> <idx>")
		return 2
	}
	idx, err := parseInt(args[1])
	if err != nil {
		printCmdMsg("lindex", fmt.Sprintf("bad idx %q: %v", args[1], err))
		return 2
	}
	var item []byte
	var ok bool
	err = sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		item, ok, e = cli.LIndex(ctx, sess.cfg.keyspace, args[0], idx)
		return e
	})
	if err != nil {
		printCmdErr("lindex", err)
		return 1
	}
	if !ok {
		printNil()
		return 1
	}
	fmt.Println(string(item))
	return 0
}

func cmdLRange(ctx context.Context, sess *session, args []string) int {
	if len(args) < 3 {
		printUsageLine("usage: lrange <name> <start> <stop>")
		return 2
	}
	start, err := parseInt(args[1])
	if err != nil {
		printCmdMsg("lrange", fmt.Sprintf("bad start %q: %v", args[1], err))
		return 2
	}
	stop, err := parseInt(args[2])
	if err != nil {
		printCmdMsg("lrange", fmt.Sprintf("bad stop %q: %v", args[2], err))
		return 2
	}
	var items [][]byte
	err = sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		items, e = cli.LRange(ctx, sess.cfg.keyspace, args[0], start, stop)
		return e
	})
	if err != nil {
		printCmdErr("lrange", err)
		return 1
	}
	for _, it := range items {
		fmt.Println(string(it))
	}
	return 0
}

func cmdHSet(ctx context.Context, sess *session, args []string) int {
	if len(args) < 3 {
		printUsageLine("usage: hset <name> <field> <value...>")
		return 2
	}
	name, field, value := args[0], args[1], strings.Join(args[2:], " ")
	err := sess.withClient(func(cli *client.Client, _ string) error {
		return cli.HSet(ctx, sess.cfg.keyspace, name, []byte(field), []byte(value))
	})
	if err != nil {
		printCmdErr("hset", err)
		return 1
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("hset %s %s", name, field))
	}
	return 0
}

func cmdHGet(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 {
		printUsageLine("usage: hget <name> <field>")
		return 2
	}
	var v []byte
	var ok bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		v, ok, e = cli.HGet(ctx, sess.cfg.keyspace, args[0], []byte(args[1]))
		return e
	})
	if err != nil {
		printCmdErr("hget", err)
		return 1
	}
	if !ok {
		printNil()
		return 1
	}
	fmt.Println(string(v))
	return 0
}

func cmdHDel(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 {
		printUsageLine("usage: hdel <name> <field>")
		return 2
	}
	err := sess.withClient(func(cli *client.Client, _ string) error {
		return cli.HDel(ctx, sess.cfg.keyspace, args[0], []byte(args[1]))
	})
	if err != nil {
		printCmdErr("hdel", err)
		return 1
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("hdel %s %s", args[0], args[1]))
	}
	return 0
}

func cmdHExists(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 {
		printUsageLine("usage: hexists <name> <field>")
		return 2
	}
	var present bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		present, e = cli.HExists(ctx, sess.cfg.keyspace, args[0], []byte(args[1]))
		return e
	})
	if err != nil {
		printCmdErr("hexists", err)
		return 1
	}
	printBool(present)
	if !present {
		return 1
	}
	return 0
}

func cmdHLen(ctx context.Context, sess *session, args []string) int {
	if len(args) < 1 {
		printUsageLine("usage: hlen <name>")
		return 2
	}
	var n int
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		n, e = cli.HLen(ctx, sess.cfg.keyspace, args[0])
		return e
	})
	if err != nil {
		printCmdErr("hlen", err)
		return 1
	}
	fmt.Println(n)
	return 0
}

func cmdHGetAll(ctx context.Context, sess *session, args []string) int {
	if len(args) < 1 {
		printUsageLine("usage: hgetall <name>")
		return 2
	}
	var all []client.HashField
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		all, e = cli.HGetAll(ctx, sess.cfg.keyspace, args[0])
		return e
	})
	if err != nil {
		printCmdErr("hgetall", err)
		return 1
	}
	for _, f := range all {
		fmt.Printf("%s\t%s\n", f.Field, f.Value)
	}
	return 0
}

func cmdIncr(ctx context.Context, sess *session, args []string) int {
	if len(args) < 1 || len(args) > 2 {
		printUsageLine("usage: incr <name> [delta]")
		return 2
	}
	delta := int64(1)
	if len(args) == 2 {
		n, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			printCmdMsg("incr", fmt.Sprintf("bad delta %q: %v", args[1], err))
			return 2
		}
		delta = n
	}
	var v int64
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		v, e = cli.Incr(ctx, sess.cfg.keyspace, args[0], delta)
		return e
	})
	if err != nil {
		printCmdErr("incr", err)
		return 1
	}
	fmt.Println(v)
	return 0
}

func cmdCGet(ctx context.Context, sess *session, args []string) int {
	if len(args) < 1 {
		printUsageLine("usage: cget <name>")
		return 2
	}
	var v int64
	var ok bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		v, ok, e = cli.CounterGet(ctx, sess.cfg.keyspace, args[0])
		return e
	})
	if err != nil {
		printCmdErr("cget", err)
		return 1
	}
	if !ok {
		printNil()
		return 1
	}
	fmt.Println(v)
	return 0
}

func cmdJSONSet(ctx context.Context, sess *session, args []string) int {
	if len(args) < 3 {
		printUsageLine("usage: jsonset <name> <path> <json...>")
		return 2
	}
	name, path, value := args[0], args[1], strings.Join(args[2:], " ")
	err := sess.withClient(func(cli *client.Client, _ string) error {
		return cli.JsonSet(ctx, sess.cfg.keyspace, name, path, []byte(value))
	})
	if err != nil {
		printCmdErr("jsonset", err)
		return 1
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("jsonset %s %s", name, path))
	}
	return 0
}

func cmdJSONGet(ctx context.Context, sess *session, args []string) int {
	if len(args) < 1 {
		printUsageLine("usage: jsonget <name> [path]")
		return 2
	}
	path := "$"
	if len(args) >= 2 {
		path = args[1]
	}
	var v []byte
	var ok bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		v, ok, e = cli.JsonGet(ctx, sess.cfg.keyspace, args[0], path)
		return e
	})
	if err != nil {
		printCmdErr("jsonget", err)
		return 1
	}
	if !ok {
		printNil()
		return 1
	}
	fmt.Println(string(v))
	return 0
}

func cmdJSONDel(ctx context.Context, sess *session, args []string) int {
	if len(args) < 1 {
		printUsageLine("usage: jsondel <name> [path]")
		return 2
	}
	path := "$"
	if len(args) >= 2 {
		path = args[1]
	}
	err := sess.withClient(func(cli *client.Client, _ string) error {
		return cli.JsonDel(ctx, sess.cfg.keyspace, args[0], path)
	})
	if err != nil {
		printCmdErr("jsondel", err)
		return 1
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("jsondel %s %s", args[0], path))
	}
	return 0
}

func cmdBitSet(ctx context.Context, sess *session, args []string) int {
	if len(args) != 3 {
		printUsageLine("usage: bitset <name> <offset> <0|1>")
		return 2
	}
	off, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		printCmdMsg("bitset", fmt.Sprintf("bad offset %q: %v", args[1], err))
		return 2
	}
	var bit bool
	switch args[2] {
	case "0":
		bit = false
	case "1":
		bit = true
	default:
		printUsageLine("usage: bitset <name> <offset> <0|1>")
		return 2
	}
	err = sess.withClient(func(cli *client.Client, _ string) error {
		return cli.BitSet(ctx, sess.cfg.keyspace, args[0], off, bit)
	})
	if err != nil {
		printCmdErr("bitset", err)
		return 1
	}
	if !sess.cfg.quiet {
		printOK(fmt.Sprintf("bitset %s %d %s", args[0], off, args[2]))
	}
	return 0
}

func cmdBitGet(ctx context.Context, sess *session, args []string) int {
	if len(args) != 2 {
		printUsageLine("usage: bitget <name> <offset>")
		return 2
	}
	off, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		printCmdMsg("bitget", fmt.Sprintf("bad offset %q: %v", args[1], err))
		return 2
	}
	var bit, ok bool
	err = sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		bit, ok, e = cli.BitGet(ctx, sess.cfg.keyspace, args[0], off)
		return e
	})
	if err != nil {
		printCmdErr("bitget", err)
		return 1
	}
	if !ok {
		printNil()
		return 1
	}
	if bit {
		fmt.Println("1")
	} else {
		fmt.Println("0")
	}
	return 0
}

func cmdBitCount(ctx context.Context, sess *session, args []string) int {
	if len(args) != 1 && len(args) != 3 {
		printUsageLine("usage: bitcount <name> [start end]")
		return 2
	}
	start, end := 0, -1
	if len(args) == 3 {
		var err error
		start, err = parseInt(args[1])
		if err != nil {
			printCmdMsg("bitcount", fmt.Sprintf("bad start %q: %v", args[1], err))
			return 2
		}
		end, err = parseInt(args[2])
		if err != nil {
			printCmdMsg("bitcount", fmt.Sprintf("bad end %q: %v", args[2], err))
			return 2
		}
	}
	var n int64
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		n, e = cli.BitCount(ctx, sess.cfg.keyspace, args[0], start, end)
		return e
	})
	if err != nil {
		printCmdErr("bitcount", err)
		return 1
	}
	fmt.Println(n)
	return 0
}

func cmdBitPos(ctx context.Context, sess *session, args []string) int {
	if len(args) != 2 && len(args) != 4 {
		printUsageLine("usage: bitpos <name> <0|1> [start end]")
		return 2
	}
	var bit bool
	switch args[1] {
	case "0":
		bit = false
	case "1":
		bit = true
	default:
		printUsageLine("usage: bitpos <name> <0|1> [start end]")
		return 2
	}
	start, end := 0, -1
	if len(args) == 4 {
		var err error
		start, err = parseInt(args[2])
		if err != nil {
			printCmdMsg("bitpos", fmt.Sprintf("bad start %q: %v", args[2], err))
			return 2
		}
		end, err = parseInt(args[3])
		if err != nil {
			printCmdMsg("bitpos", fmt.Sprintf("bad end %q: %v", args[3], err))
			return 2
		}
	}
	var pos int64
	var found bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		pos, found, e = cli.BitPos(ctx, sess.cfg.keyspace, args[0], bit, start, end)
		return e
	})
	if err != nil {
		printCmdErr("bitpos", err)
		return 1
	}
	if !found {
		printNil()
		return 1
	}
	fmt.Println(pos)
	return 0
}

func cmdHLLAdd(ctx context.Context, sess *session, args []string) int {
	if len(args) < 2 {
		printUsageLine("usage: hlladd <name> <item...>")
		return 2
	}
	name := args[0]
	for _, item := range args[1:] {
		err := sess.withClient(func(cli *client.Client, _ string) error {
			return cli.HLLAdd(ctx, sess.cfg.keyspace, name, []byte(item))
		})
		if err != nil {
			printCmdErr("hlladd", err)
			return 1
		}
		if !sess.cfg.quiet {
			printOK(fmt.Sprintf("hlladd %s %s", name, item))
		}
	}
	return 0
}

func cmdHLLCount(ctx context.Context, sess *session, args []string) int {
	if len(args) != 1 {
		printUsageLine("usage: hllcount <name>")
		return 2
	}
	var n uint64
	var ok bool
	err := sess.withClient(func(cli *client.Client, _ string) error {
		var e error
		n, ok, e = cli.HLLCount(ctx, sess.cfg.keyspace, args[0])
		return e
	})
	if err != nil {
		printCmdErr("hllcount", err)
		return 1
	}
	if !ok {
		printNil()
		return 1
	}
	fmt.Println(n)
	return 0
}

func parseFloat(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

func parseInt(s string) (int, error) {
	n, err := strconv.Atoi(s)
	return n, err
}

func cmdPing(ctx context.Context, sess *session) int {
	err := sess.withClient(func(cli *client.Client, addr string) error {
		_, err := cli.Get(ctx, sess.cfg.keyspace, "__sc_ping__")
		if err != nil && !errors.Is(err, client.ErrNotFound) {
			return err
		}
		fmt.Printf("%s %s: %s\n", paint(os.Stdout, ansiCyan, "cache"), addr, paint(os.Stdout, ansiGreen, "ok"))
		return nil
	})
	if err != nil {
		printCmdErr("cache", err)
		return 1
	}

	code, body, aerr, used := adminGET(ctx, sess.cfg, "/healthz")
	if aerr != nil {
		fmt.Printf("%s: %s (%v)\n", paint(os.Stdout, ansiCyan, "admin"), paint(os.Stdout, ansiYellow, "unreachable"), aerr)
		return 0
	}
	fmt.Printf("%s %s: HTTP %d\n", paint(os.Stdout, ansiCyan, "admin"), used, code)
	if sess.cfg.jsonOut && len(body) > 0 {
		os.Stdout.Write(body)
	}
	return 0
}

func cmdAdmin(ctx context.Context, cfg *config, path string) int {
	code, body, err, used := adminGET(ctx, cfg, path)
	if err != nil {
		printCmdMsg("admin", fmt.Sprintf("%s: %v", path, err))
		return 1
	}
	if code >= 400 {
		printCmdMsg("admin", fmt.Sprintf("%s HTTP %d", used, code))
		writeBody(body)
		return 1
	}
	if cfg.jsonOut {
		writeBody(body)
		return 0
	}
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		writeBody(body)
		return 0
	}
	if m, ok := v.(map[string]any); ok {
		m["_admin"] = used
		v = m
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	return 0
}

func writeBody(body []byte) {
	os.Stdout.Write(body)
	if len(body) == 0 || body[len(body)-1] != '\n' {
		fmt.Println()
	}
}

// adminGET tries each admin seed until one responds.
func adminGET(ctx context.Context, cfg *config, path string) (code int, body []byte, err error, used string) {
	admins := cfg.admins
	if len(admins) == 0 {
		return 0, nil, fmt.Errorf("no admin seeds configured"), ""
	}
	var errs []string
	for _, a := range admins {
		code, body, err = adminGETOne(ctx, a, path)
		if err == nil {
			return code, body, nil, a
		}
		errs = append(errs, fmt.Sprintf("%s: %v", a, err))
	}
	return 0, nil, fmt.Errorf("all admin seed(s) failed:\n  %s", strings.Join(errs, "\n  ")), ""
}

func adminGETOne(ctx context.Context, admin, path string) (int, []byte, error) {
	base := admin
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	base = strings.TrimRight(base, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
	if err != nil {
		return 0, nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, body, nil
}

func readFileOrStdin(path string) ([]byte, error) {
	if path == "-" {
		return io.ReadAll(io.LimitReader(os.Stdin, 32<<20))
	}
	return os.ReadFile(path)
}

func isMostlyPrintable(b []byte) bool {
	if len(b) == 0 {
		return true
	}
	bad := 0
	for _, c := range b {
		if c == '\n' || c == '\r' || c == '\t' {
			continue
		}
		if c < 32 || c == 127 {
			bad++
		}
	}
	return bad*10 <= len(b)
}
