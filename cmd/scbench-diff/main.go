// Command scbench-diff compares two scbench JSON reports (and optional go test -bench logs)
// and writes a Markdown summary for PR comments.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type suiteReport struct {
	GeneratedAt string      `json:"generated_at"`
	GitSHA      string      `json:"git_sha"`
	GOMAXPROCS  int         `json:"gomaxprocs"`
	Runs        []runRecord `json:"runs"`
}

type trialRow struct {
	OpsPerSec float64 `json:"ops_per_sec"`
	P50       int64   `json:"p50_ns"`
	P99       int64   `json:"p99_ns"`
}

type runRecord struct {
	Op              string     `json:"op"`
	Path            string     `json:"path"`
	Nodes           int        `json:"nodes"`
	Concurrency     int        `json:"concurrency"`
	Dist            string     `json:"dist"`
	MedianOpsPerSec float64    `json:"median_ops_per_sec"`
	MedianP50       int64      `json:"median_p50_ns"`
	MedianP99       int64      `json:"median_p99_ns"`
	Trials          []trialRow `json:"trials"`
}

func (r runRecord) avgOps() float64 {
	if n := len(r.Trials); n > 0 {
		var s float64
		for _, t := range r.Trials {
			s += t.OpsPerSec
		}
		return s / float64(n)
	}
	return r.MedianOpsPerSec
}

func (r runRecord) avgP50() int64 {
	return avgNs(r.Trials, func(t trialRow) int64 { return t.P50 }, r.MedianP50)
}
func (r runRecord) avgP99() int64 {
	return avgNs(r.Trials, func(t trialRow) int64 { return t.P99 }, r.MedianP99)
}

func avgNs(trials []trialRow, get func(trialRow) int64, fallback int64) int64 {
	if len(trials) == 0 {
		return fallback
	}
	var s float64
	for _, t := range trials {
		s += float64(get(t))
	}
	return int64(s / float64(len(trials)))
}

func main() {
	oldJSON := flag.String("old", "", "previous smoke.json (main)")
	newJSON := flag.String("new", "", "current smoke.json")
	oldMicro := flag.String("old-micro", "", "previous micro.txt")
	newMicro := flag.String("new-micro", "", "current micro.txt")
	out := flag.String("out", "", "write markdown here (default stdout)")
	flag.Parse()
	if *newJSON == "" {
		fmt.Fprintln(os.Stderr, "usage: scbench-diff -new smoke.json [-old prev/smoke.json] [-old-micro ...] [-new-micro ...] [-out comment.md]")
		os.Exit(2)
	}
	md, err := render(*oldJSON, *newJSON, *oldMicro, *newMicro)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if *out == "" {
		fmt.Print(md)
		return
	}
	if err := os.WriteFile(*out, []byte(md), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func render(oldPath, newPath, oldMicro, newMicro string) (string, error) {
	cur, err := readSuite(newPath)
	if err != nil {
		return "", err
	}
	var prev *suiteReport
	if oldPath != "" {
		p, err := readSuite(oldPath)
		if err != nil {
			return "", fmt.Errorf("old report: %w", err)
		}
		prev = &p
	}

	var b strings.Builder
	b.WriteString("<!-- supercache-bench-comment -->\n")
	b.WriteString("## SuperCache bench vs `main`\n\n")
	b.WriteString("Each number is the **average of 3 runs** on `ubuntu-latest`. Hosted runners are still noisy; treat &lt;15–20% moves as noise.\n\n")
	if prev == nil {
		b.WriteString("_No previous `bench-smoke` artifact on `main` yet — showing this PR only._\n\n")
	} else {
		fmt.Fprintf(&b, "Baseline: `%s` (%s) → this run: `%s` (%s)\n\n",
			shortSHA(prev.GitSHA), prev.GeneratedAt, shortSHA(cur.GitSHA), cur.GeneratedAt)
	}

	b.WriteString("### scbench smoke\n\n")
	b.WriteString("| cell | ops/s (main → PR) | Δ ops/s | p50 | p99 |\n")
	b.WriteString("|------|------------------:|--------:|----:|----:|\n")
	keys := collectKeys(cur, prev)
	for _, k := range keys {
		n := findRun(cur.Runs, k)
		var o *runRecord
		if prev != nil {
			o = findRun(prev.Runs, k)
		}
		if n == nil {
			continue
		}
		label := k
		if o == nil {
			fmt.Fprintf(&b, "| `%s` | — → **%.0f** | n/a | %s | %s |\n",
				label, n.avgOps(), dur(n.avgP50()), dur(n.avgP99()))
			continue
		}
		dOps := pct(o.avgOps(), n.avgOps(), true)
		fmt.Fprintf(&b, "| `%s` | %.0f → **%.0f** | %s | %s → %s | %s → %s |\n",
			label, o.avgOps(), n.avgOps(), dOps,
			dur(o.avgP50()), dur(n.avgP50()),
			dur(o.avgP99()), dur(n.avgP99()))
	}
	b.WriteString("\nΔ ops/s: **green-ish** is higher throughput. Latency: lower is better.\n\n")

	if newMicro != "" {
		b.WriteString("### `go test -bench` (avg ns/op over 3 counts)\n\n")
		curM, err := parseMicro(newMicro)
		if err != nil {
			fmt.Fprintf(&b, "_could not parse new micro.txt: %v_\n\n", err)
		} else {
			var oldM map[string]microRow
			if oldMicro != "" {
				oldM, _ = parseMicro(oldMicro)
			}
			b.WriteString("| benchmark | ns/op (main → PR) | Δ | allocs/op |\n")
			b.WriteString("|-----------|------------------:|--:|----------:|\n")
			names := make([]string, 0, len(curM))
			for n := range curM {
				names = append(names, n)
			}
			sort.Strings(names)
			for _, name := range names {
				nm := curM[name]
				om, ok := oldM[name]
				if !ok {
					fmt.Fprintf(&b, "| `%s` | — → **%.0f** | n/a | %.0f |\n", name, nm.nsPerOp, nm.allocs)
					continue
				}
				// For ns/op, lower is better — invert the arrow sense vs ops/s.
				fmt.Fprintf(&b, "| `%s` | %.0f → **%.0f** | %s | %.0f → %.0f |\n",
					name, om.nsPerOp, nm.nsPerOp, pct(om.nsPerOp, nm.nsPerOp, false), om.allocs, nm.allocs)
			}
			b.WriteString("\n")
		}
	}
	b.WriteString("<sub>Posted by CI bench job. Not a merge gate.</sub>\n")
	return b.String(), nil
}

func readSuite(path string) (suiteReport, error) {
	var s suiteReport
	b, err := os.ReadFile(path)
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(b, &s); err != nil {
		return s, err
	}
	return s, nil
}

func cellKey(r runRecord) string {
	path := r.Path
	if path == "" {
		path = r.Op
	}
	dist := r.Dist
	if dist == "" {
		dist = "uniform"
	}
	nodes := r.Nodes
	if nodes == 0 {
		nodes = 1
	}
	return fmt.Sprintf("%s/%s n=%d c=%d %s", r.Op, path, nodes, r.Concurrency, dist)
}

func collectKeys(cur suiteReport, prev *suiteReport) []string {
	seen := map[string]struct{}{}
	var keys []string
	add := func(runs []runRecord) {
		for _, r := range runs {
			k := cellKey(r)
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			keys = append(keys, k)
		}
	}
	add(cur.Runs)
	if prev != nil {
		add(prev.Runs)
	}
	sort.Strings(keys)
	return keys
}

func findRun(runs []runRecord, key string) *runRecord {
	for i := range runs {
		if cellKey(runs[i]) == key {
			return &runs[i]
		}
	}
	return nil
}

// pct is (new-old)/old. If higherBetter, a large drop is highlighted as worse.
func pct(old, new float64, higherBetter bool) string {
	if old == 0 {
		return "n/a"
	}
	p := (new - old) / old * 100
	sign := "+"
	if p < 0 {
		sign = ""
	}
	s := fmt.Sprintf("%s%.1f%%", sign, p)
	worse := (higherBetter && p <= -20) || (!higherBetter && p >= 20)
	if worse {
		return "**" + s + "** worse"
	}
	if (higherBetter && p >= 20) || (!higherBetter && p <= -20) {
		return "**" + s + "**"
	}
	return s
}

func dur(ns int64) string {
	if ns <= 0 {
		return "—"
	}
	return time.Duration(ns).Round(time.Microsecond).String()
}

func shortSHA(s string) string {
	if len(s) > 7 {
		return s[:7]
	}
	if s == "" {
		return "unknown"
	}
	return s
}

type microRow struct {
	nsPerOp float64
	allocs  float64
}

var benchLine = regexp.MustCompile(`^(Benchmark\S+)-\d+\s+\d+\s+([0-9.]+)\s+ns/op(?:.*?([0-9.]+)\s+allocs/op)?`)

func parseMicro(path string) (map[string]microRow, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	sum := map[string]microRow{}
	n := map[string]int{}
	for _, line := range strings.Split(string(b), "\n") {
		m := benchLine.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		ns, _ := strconv.ParseFloat(m[2], 64)
		var al float64
		if m[3] != "" {
			al, _ = strconv.ParseFloat(m[3], 64)
		}
		name := m[1]
		s := sum[name]
		s.nsPerOp += ns
		s.allocs += al
		sum[name] = s
		n[name]++
	}
	out := map[string]microRow{}
	for name, s := range sum {
		c := float64(n[name])
		if c == 0 {
			continue
		}
		out[name] = microRow{nsPerOp: s.nsPerOp / c, allocs: s.allocs / c}
	}
	return out, nil
}
