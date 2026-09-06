package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"
)

func runREPL(cfg *config) int {
	sess := newSession(cfg)
	defer sess.Close()

	applyJSONColor(cfg.jsonOut)

	// Eager dial so the first prompt shows the connected seed.
	if _, addr, err := sess.Client(); err != nil {
		printWarn(fmt.Sprintf("not connected yet: %v", err))
		fmt.Fprintf(os.Stderr, "seeds: %s\n", strings.Join(cfg.addrs, ", "))
	} else {
		fmt.Printf("%s %s  keyspace=%s  seeds=%d\n",
			paint(os.Stdout, ansiGreen, "connected"), addr, cfg.keyspace, len(cfg.addrs))
	}
	fmt.Println(paint(os.Stdout, ansiDim, "type help for commands, quit to exit"))

	in := bufio.NewScanner(os.Stdin)
	// Allow long put lines
	in.Buffer(make([]byte, 0, 64*1024), 4<<20)

	for {
		addr := sess.ConnectedAddr()
		if addr == "" {
			addr = "?"
		}
		fmt.Print(colorPrompt(cfg.keyspace, shortAddr(addr)))
		if !in.Scan() {
			fmt.Println()
			break
		}
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}
		args, err := tokenize(line)
		if err != nil {
			printCmdErr("parse", err)
			continue
		}
		if len(args) == 0 {
			continue
		}
		cmd := strings.ToLower(args[0])
		rest := args[1:]

		switch cmd {
		case "quit", "exit", "q":
			return 0
		case "help", "?":
			printREPLHelp()
			continue
		case "version":
			fmt.Printf("sc %s\n", version)
			continue
		case "keyspace", "use", "ks":
			if len(rest) != 1 {
				printUsageLine("usage: keyspace <name>")
				continue
			}
			cfg.keyspace = rest[0]
			fmt.Printf("keyspace = %s\n", paint(os.Stdout, ansiBold, cfg.keyspace))
			continue
		case "seeds", "addrs":
			fmt.Printf("cache seeds (%d):\n", len(cfg.addrs))
			cur := sess.ConnectedAddr()
			for i, a := range cfg.addrs {
				mark := "  "
				if a == cur {
					mark = "* "
				}
				fmt.Printf("%s[%d] %s\n", mark, i, a)
			}
			if len(cfg.admins) > 0 {
				fmt.Printf("admin seeds (%d): %s\n", len(cfg.admins), strings.Join(cfg.admins, ", "))
			}
			continue
		case "connect", "reconnect":
			sess.Invalidate()
			// If arg given, try that seed first by rotating list order temporarily.
			if len(rest) == 1 {
				target := rest[0]
				found := -1
				for i, a := range cfg.addrs {
					if a == target {
						found = i
						break
					}
				}
				if found < 0 {
					// allow bare host match
					for i, a := range cfg.addrs {
						if strings.Contains(a, target) {
							found = i
							break
						}
					}
				}
				if found >= 0 {
					sess.mu.Lock()
					sess.seedIdx = found
					sess.mu.Unlock()
				} else {
					printCmdMsg("connect", fmt.Sprintf("unknown seed %q (use seeds to list)", target))
					continue
				}
			}
			if _, addr, err := sess.Client(); err != nil {
				printCmdErr("connect", err)
			} else {
				fmt.Printf("%s %s\n", paint(os.Stdout, ansiGreen, "connected"), addr)
			}
			continue
		case "ttl":
			if len(rest) == 0 {
				if cfg.ttlSet {
					fmt.Printf("ttl = %s (explicit)\n", cfg.ttl)
				} else {
					fmt.Println("ttl = (keyspace default)")
				}
				continue
			}
			if rest[0] == "default" || rest[0] == "off" {
				cfg.ttlSet = false
				cfg.ttl = 0
				fmt.Println("ttl = (keyspace default)")
				continue
			}
			d, err := time.ParseDuration(rest[0])
			if err != nil {
				printCmdErr("ttl", err)
				continue
			}
			cfg.ttl = d
			cfg.ttlSet = true
			fmt.Printf("ttl = %s\n", d)
			continue
		case "json":
			if len(rest) == 1 {
				switch strings.ToLower(rest[0]) {
				case "on", "1", "true":
					cfg.jsonOut = true
				case "off", "0", "false":
					cfg.jsonOut = false
				default:
					printUsageLine("usage: json [on|off]")
					continue
				}
			}
			applyJSONColor(cfg.jsonOut)
			fmt.Printf("json = %v\n", cfg.jsonOut)
			continue
		case "timeout":
			if len(rest) == 1 {
				d, err := time.ParseDuration(rest[0])
				if err != nil {
					printCmdErr("timeout", err)
					continue
				}
				cfg.timeout = d
			}
			fmt.Printf("timeout = %s\n", cfg.timeout)
			continue
		case "clear":
			fmt.Print("\033[H\033[2J")
			continue
		}

		// Reset per-command file path (REPL doesn't use -file from CLI mid-session
		// unless user types put k -file x which we handle below).
		cfg.filePath = ""
		cfg.base64 = false

		// Inline flags for a single REPL command: put k -file x, get k -base64, etc.
		cmdArgs, err := applyREPLFlags(cfg, rest)
		if err != nil {
			printCmdErr("sc", err)
			continue
		}
		applyJSONColor(cfg.jsonOut)

		ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
		_ = dispatch(ctx, sess, cmd, cmdArgs)
		cancel()
	}
	if err := in.Err(); err != nil {
		printCmdErr("stdin", err)
		return 1
	}
	return 0
}

func shortAddr(addr string) string {
	// 127.0.0.1:9000 -> :9000 if localhost-ish keeps host when non-local
	if strings.HasPrefix(addr, "127.0.0.1:") {
		return addr[len("127.0.0.1"):]
	}
	if strings.HasPrefix(addr, "localhost:") {
		return addr[len("localhost"):]
	}
	return addr
}

// applyREPLFlags peels common flags out of REPL args for one command.
func applyREPLFlags(cfg *config, args []string) ([]string, error) {
	var out []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch a {
		case "-file":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("-file needs a path")
			}
			cfg.filePath = args[i+1]
			i++
		case "-base64":
			cfg.base64 = true
		case "-json":
			cfg.jsonOut = true
		case "-q":
			cfg.quiet = true
		case "-raw":
			cfg.raw = true
		case "-ttl":
			if i+1 >= len(args) {
				return nil, fmt.Errorf("-ttl needs a duration")
			}
			d, err := time.ParseDuration(args[i+1])
			if err != nil {
				return nil, fmt.Errorf("-ttl: %w", err)
			}
			cfg.ttl = d
			cfg.ttlSet = true
			i++
		case "-no-expiry":
			cfg.ttl = 0
			cfg.ttlSet = true
		default:
			if strings.HasPrefix(a, "-") && a != "-" {
				return nil, fmt.Errorf("unknown flag %s", a)
			}
			out = append(out, a)
		}
	}
	return out, nil
}
