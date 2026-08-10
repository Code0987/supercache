package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/Code0987/supercache/internal/testcluster"
	"github.com/Code0987/supercache/pkg/datasource"
	"github.com/Code0987/supercache/pkg/keyspace"
)

type matrixFile struct {
	Tier           string       `yaml:"tier"`
	Gomaxprocs     int          `yaml:"gomaxprocs"`
	Trials         int          `yaml:"trials"`
	Duration       string       `yaml:"duration"`
	Warmup         string       `yaml:"warmup"`
	Keys           int          `yaml:"keys"`
	ValueBytes     int          `yaml:"value_bytes"`
	Dist           string       `yaml:"dist"`
	Seed           uint64       `yaml:"seed"`
	Embed          bool         `yaml:"embed"`
	CollectRuntime bool         `yaml:"collect_runtime"`
	Conns          int          `yaml:"conns"`
	Prefix         string       `yaml:"prefix"`
	Cells          []matrixCell `yaml:"cells"`
}

type matrixCell struct {
	Op          string `yaml:"op"`
	Path        string `yaml:"path"`
	Nodes       int    `yaml:"nodes"`
	Concurrency int    `yaml:"concurrency"`
	Conns       int    `yaml:"conns"`
	Sticky      bool   `yaml:"sticky"`
	Dist        string `yaml:"dist"`
	Keys        int    `yaml:"keys"`
	Trials      int    `yaml:"trials"`
	Duration    string `yaml:"duration"`
	Warmup      string `yaml:"warmup"`
}

type resolvedCell struct {
	Op, Path, Dist, Prefix                              string
	Nodes, Concurrency, Conns, Keys, Trials, ValueBytes int
	Sticky, Embed, CollectRuntime, RequireHit           bool
	Duration, Warmup                                    time.Duration
	Seed                                                uint64
}

func findRepoFile(rel string) string {
	if filepath.IsAbs(rel) {
		return rel
	}
	if _, err := os.Stat(rel); err == nil {
		return rel
	}
	wd, err := os.Getwd()
	if err != nil {
		return rel
	}
	for d := wd; ; d = filepath.Dir(d) {
		p := filepath.Join(d, rel)
		if _, err := os.Stat(p); err == nil {
			return p
		}
		if d == filepath.Dir(d) {
			break
		}
	}
	return rel
}

func loadMatrixFile(path string) (matrixFile, error) {
	var mf matrixFile
	b, err := os.ReadFile(findRepoFile(path))
	if err != nil {
		return mf, err
	}
	if err := yaml.Unmarshal(b, &mf); err != nil {
		return mf, err
	}
	return mf, nil
}

func presetMatrix(tier string) (matrixFile, error) {
	switch strings.ToLower(tier) {
	case "smoke":
		return loadMatrixFile("bench/ci-smoke.yaml")
	case "laptop":
		return loadMatrixFile("bench/laptop.yaml")
	case "full":
		return loadMatrixFile("bench/local-full.yaml")
	default:
		return matrixFile{}, fmt.Errorf("unknown -tier %q", tier)
	}
}

func resolveCells(mf matrixFile) ([]resolvedCell, error) {
	dur, err := parseDur(mf.Duration, 15*time.Second)
	if err != nil {
		return nil, err
	}
	warm, err := parseDur(mf.Warmup, 5*time.Second)
	if err != nil {
		return nil, err
	}
	if mf.Trials < 1 {
		mf.Trials = 1
	}
	if mf.Keys < 1 {
		mf.Keys = 5000
	}
	if mf.ValueBytes < 1 {
		mf.ValueBytes = 256
	}
	if mf.Conns < 1 {
		mf.Conns = 1
	}
	if mf.Dist == "" {
		mf.Dist = "uniform"
	}
	if mf.Prefix == "" {
		mf.Prefix = "scbench:"
	}
	out := make([]resolvedCell, 0, len(mf.Cells))
	for i, c := range mf.Cells {
		rc := resolvedCell{
			Op: strings.ToLower(c.Op), Path: strings.ToLower(c.Path),
			Dist: mf.Dist, Prefix: mf.Prefix,
			Nodes: c.Nodes, Concurrency: c.Concurrency, Conns: mf.Conns,
			Keys: mf.Keys, Trials: mf.Trials, ValueBytes: mf.ValueBytes,
			Sticky: c.Sticky, Embed: mf.Embed, CollectRuntime: mf.CollectRuntime,
			Duration: dur, Warmup: warm, Seed: mf.Seed,
		}
		if c.Conns > 0 {
			rc.Conns = c.Conns
		}
		if c.Keys > 0 {
			rc.Keys = c.Keys
		}
		if c.Trials > 0 {
			rc.Trials = c.Trials
		}
		if c.Dist != "" {
			rc.Dist = c.Dist
		}
		if c.Duration != "" {
			rc.Duration, err = time.ParseDuration(c.Duration)
			if err != nil {
				return nil, fmt.Errorf("cell %d duration: %w", i, err)
			}
		}
		if c.Warmup != "" {
			rc.Warmup, err = time.ParseDuration(c.Warmup)
			if err != nil {
				return nil, fmt.Errorf("cell %d warmup: %w", i, err)
			}
		}
		if rc.Nodes < 1 {
			rc.Nodes = 1
		}
		if rc.Concurrency < 1 {
			return nil, fmt.Errorf("cell %d: concurrency required", i)
		}
		if rc.Path == "" {
			rc.Path = defaultPath(rc.Op)
		}
		if rc.Path == "hit" || rc.Path == "get-hit" {
			rc.Path = "hit"
			rc.RequireHit = true
			if rc.Op == "" {
				rc.Op = "get"
			}
		}
		if rc.Path == "put" && rc.Op == "" {
			rc.Op = "set"
		}
		if strings.HasPrefix(rc.Path, "miss") && rc.Op == "" {
			rc.Op = "miss"
		}
		if err := validateCell(rc); err != nil {
			return nil, fmt.Errorf("cell %d: %w", i, err)
		}
		out = append(out, rc)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("matrix has no cells")
	}
	return out, nil
}

func defaultPath(op string) string {
	switch op {
	case "get":
		return "hit"
	case "set":
		return "put"
	case "delete":
		return "delete"
	case "miss":
		return "miss-cacheonly"
	default:
		return op
	}
}

func validateCell(c resolvedCell) error {
	switch c.Op {
	case "get", "set", "mixed", "delete", "miss":
	default:
		return fmt.Errorf("invalid op %q", c.Op)
	}
	switch c.Path {
	case "hit", "put", "delete", "miss-cacheonly", "miss-loadthrough", "mixed":
	default:
		return fmt.Errorf("invalid path %q", c.Path)
	}
	if c.Nodes != 1 && c.Nodes != 3 && c.Nodes != 10 {
		return fmt.Errorf("nodes must be 1, 3, or 10")
	}
	if c.Path == "miss-loadthrough" && !c.Embed {
		return fmt.Errorf("miss-loadthrough needs embed")
	}
	return nil
}

func parseDur(s string, def time.Duration) (time.Duration, error) {
	if s == "" {
		return def, nil
	}
	return time.ParseDuration(s)
}

func runMatrix(ctx context.Context, mf matrixFile, sampleCap int, jsonNote string) ([]runRecord, error) {
	if mf.Gomaxprocs > 0 {
		runtime.GOMAXPROCS(mf.Gomaxprocs)
	}
	cells, err := resolveCells(mf)
	if err != nil {
		return nil, err
	}
	var runs []runRecord
	for i, cell := range cells {
		fmt.Printf("cell %d/%d op=%s path=%s nodes=%d conc=%d sticky=%v\n",
			i+1, len(cells), cell.Op, cell.Path, cell.Nodes, cell.Concurrency, cell.Sticky)
		rec, err := runEmbedCell(ctx, cell, sampleCap)
		if err != nil {
			return runs, fmt.Errorf("cell %d: %w", i, err)
		}
		printRunSummary(rec)
		runs = append(runs, rec)
	}
	_ = jsonNote
	return runs, nil
}

func runEmbedCell(ctx context.Context, cell resolvedCell, sampleCap int) (runRecord, error) {
	value := make([]byte, cell.ValueBytes)
	for i := range value {
		value[i] = byte('a' + i%26)
	}
	ksList := []keyspace.Config{testcluster.CacheOnlyBench()}
	lt := cell.Path == "miss-loadthrough"
	if lt {
		src := datasource.Func(func(_ context.Context, _ string) ([]byte, error) {
			out := make([]byte, len(value))
			copy(out, value)
			return out, nil
		})
		ksList = []keyspace.Config{testcluster.LoadThroughBench(src)}
	}
	cl, err := testcluster.Start(testcluster.Config{
		Nodes:     cell.Nodes,
		Keyspaces: ksList,
	})
	if err != nil {
		return runRecord{}, err
	}
	defer cl.Close()

	addrs := cl.CacheAddrs()
	if cell.Sticky && len(addrs) > 1 {
		addrs = addrs[:1]
	}
	ksName := "bench"
	if lt {
		ksName = "benchlt"
	}
	store, err := openSCPool(ctx, addrs, ksName, cell.Conns)
	if err != nil {
		return runRecord{}, err
	}
	defer store.Close()

	if cell.Path == "hit" || cell.Op == "delete" {
		if err := cl.PrefillAll(ctx, ksName, cell.Prefix, cell.Keys, value); err != nil {
			return runRecord{}, err
		}
		if cell.Path == "hit" {
			if err := cl.VerifyLocalHits(ctx, ksName, cell.Prefix, cell.Keys, 0); err != nil {
				return runRecord{}, fmt.Errorf("verify hits: %w", err)
			}
		}
	}

	var dkind distKind
	switch cell.Dist {
	case "zipf":
		dkind = distZipf
	default:
		dkind = distUniform
	}
	seq := &atomic.Int64{}
	cfg := loadConfig{
		op: cell.Op, prefix: cell.Prefix, keys: cell.Keys, value: value,
		concurrency: cell.Concurrency, duration: cell.Duration, readRatio: 0.95,
		dist: dkind, zipfS: 1.1, seed: cell.Seed,
		collectRuntime: cell.CollectRuntime,
		requireHit:     cell.RequireHit && cell.Op == "get",
		sampleCap:      sampleCap,
		uniqueKeys:     lt,
		seq:            seq,
	}
	if cell.Warmup > 0 {
		wcfg := cfg
		wcfg.duration = cell.Warmup
		_, _ = runLoad(ctx, store, wcfg)
	}
	var trialsOut []trialResult
	for t := 1; t <= cell.Trials; t++ {
		cfg.duration = cell.Duration
		cfg.seed = cell.Seed + uint64(t)*0x100000001b3
		res, err := runLoad(ctx, store, cfg)
		if err != nil {
			return runRecord{}, err
		}
		printTrial("supercache", cell.Op, t, cell.Trials, res)
		trialsOut = append(trialsOut, res)
	}
	agg := aggregateTrials(trialsOut)
	fs := cl.FanoutStats()
	rec := runRecord{
		Backend:         "supercache",
		Addr:            strings.Join(cl.CacheAddrs(), ","),
		Op:              cell.Op,
		Keys:            cell.Keys,
		ValueBytes:      cell.ValueBytes,
		Concurrency:     cell.Concurrency,
		Dist:            cell.Dist,
		Trials:          trialsOut,
		MedianOpsPerSec: agg.medOps,
		MedianP50:       agg.medP50,
		MedianP95:       agg.medP95,
		MedianP99:       agg.medP99,
		MedianP999:      agg.medP999,
		MinOpsPerSec:    agg.minOps,
		MaxOpsPerSec:    agg.maxOps,
		MedianProc:      agg.medProc,
		Nodes:           cell.Nodes,
		Conns:           cell.Conns,
		Sticky:          cell.Sticky,
		Path:            cell.Path,
		Embed:           true,
		FanoutErrors:    fs.Errors,
		FanoutDropped:   fs.Dropped,
		HintsFlushed:    fs.HintsFlushed,
		HintsDropped:    fs.HintsDropped,
	}
	if cell.RequireHit && rec.MedianOpsPerSec == 0 && cell.Path == "hit" && cell.Nodes == 1 {
		return rec, fmt.Errorf("1-node get-hit produced 0 ops/s")
	}
	return rec, nil
}
