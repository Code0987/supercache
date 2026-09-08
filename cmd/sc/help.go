package main

import (
	"fmt"
	"os"
)

type helpGroup struct {
	label string
	cmds  string
}

var cacheHelpGroups = []helpGroup{
	{"kv", "get  put  del"},
	{"set", "sadd  srem  sismember  scard  smembers"},
	{"bloom", "add|test"},
	{"zset", "zadd  zrem  zscore  zcard  zrange  zrangebyscore"},
	{"geo", "geoadd  georem  geopos  geocard  geodist  georadius"},
	{"list", "lpush  rpush  lpop  rpop  llen  lindex  lrange"},
	{"hash", "hset  hget  hdel  hexists  hlen  hgetall"},
	{"counter", "incr  cget"},
	{"json", "jsonset  jsonget  jsondel"},
	{"bitmap", "bitset  bitget  bitcount  bitpos"},
	{"hll", "hlladd  hllcount"},
	{"topk", "topkadd  topklist"},
	{"cms", "cmsincr  cmsquery"},
	{"vector", "vadd  vrem  vsim  vcard  vdim  vemb"},
	{"stream", "xadd  xlen  xrange  xrevrange  xdel  xtrim"},
	{"admin", "ping  peers  keyspaces  metrics  health  ready"},
}

func printUsage() {
	printAppHelp(os.Stderr, false)
}

func printREPLHelp() {
	printAppHelp(os.Stdout, true)
}

func printAppHelp(w *os.File, repl bool) {
	if !repl {
		fmt.Fprintln(w, paint(w, ansiBold+ansiCyan, fmt.Sprintf("sc — SuperCache CLI (%s)", version)))
		fmt.Fprintln(w)
		printHelpHeading(w, "Usage")
		fmt.Fprintln(w, "  sc [flags] <command> [args]")
		fmt.Fprintln(w, "  sc                            # REPL on a TTY")
		fmt.Fprintln(w)
	}

	printHelpHeading(w, "Commands")
	printHelpGroups(w, cacheHelpGroups)
	fmt.Fprintln(w)
	printHelpHeading(w, "REPL")
	fmt.Fprintln(w, "  keyspace  seeds  connect  ttl  json  timeout  clear  help  version  quit")

	if !repl {
		fmt.Fprintln(w)
		printHelpHeading(w, "Flags")
		fmt.Fprintln(w, "  -addr  -admin  -keyspace  -timeout  -ttl  -file  -json  -q  -raw  -base64")
		fmt.Fprintln(w, "  "+paint(w, ansiDim, "env  SC_ADDR  SC_ADMIN  SC_KEYSPACE  SC_TLS_*"))
		fmt.Fprintln(w)
		printHelpHeading(w, "Examples")
		fmt.Fprintln(w, `  sc put greeting "hello"`)
		fmt.Fprintln(w, "  sc get greeting")
		fmt.Fprintln(w, "  sc -keyspace set sadd features dark_mode")
	}

	fmt.Fprintln(w)
	fmt.Fprintln(w, paint(w, ansiDim, "run a command with no args for usage.  miss=(nil)  mutations=OK <verb>"))
}

func printHelpHeading(w *os.File, s string) {
	fmt.Fprintln(w, paint(w, ansiBold+ansiCyan, s))
}

func printHelpGroups(w *os.File, groups []helpGroup) {
	width := 0
	for _, g := range groups {
		if n := len(g.label); n > width {
			width = n
		}
	}
	for _, g := range groups {
		label := paint(w, ansiCyan, fmt.Sprintf("%-*s", width, g.label))
		fmt.Fprintf(w, "  %s  %s\n", label, g.cmds)
	}
}
