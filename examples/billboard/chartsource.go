package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Code0987/supercache/pkg/datasource"
)

// ChartSource is a mock expensive chart aggregator (source-of-truth).
// Keys:
//   chart:global
//   chart:genre:<name>
//   track:<id>
type ChartSource struct {
	log       *log.Logger
	latency   time.Duration
	failEvery int64 // every Nth load fails (0 = off); for breaker demos

	mu         sync.Mutex
	generation atomic.Int64 // bumped to reshuffle scores
	loads      atomic.Int64
	fails      atomic.Int64
	inFlight   atomic.Int64
}

func NewChartSource(logger *log.Logger, latency time.Duration) *ChartSource {
	if logger == nil {
		logger = log.Default()
	}
	return &ChartSource{log: logger, latency: latency}
}

func (s *ChartSource) Stats() (loads, fails, gen int64) {
	return s.loads.Load(), s.fails.Load(), s.generation.Load()
}

// BumpGeneration forces the next chart load to recompute rankings.
func (s *ChartSource) BumpGeneration(reason string) {
	g := s.generation.Add(1)
	s.log.Printf("[SoT] generation → %d (%s)", g, reason)
}

func (s *ChartSource) Load(ctx context.Context, key string) ([]byte, error) {
	n := s.loads.Add(1)
	cur := s.inFlight.Add(1)
	defer s.inFlight.Add(-1)

	s.log.Printf("[SoT] LOAD begin key=%q load#=%d in_flight=%d latency=%s", key, n, cur, s.latency)

	if s.failEvery > 0 && n%s.failEvery == 0 {
		s.fails.Add(1)
		err := fmt.Errorf("chart aggregator overloaded on load#%d", n)
		s.log.Printf("[SoT] LOAD FAIL key=%q err=%v", key, err)
		return nil, err
	}

	start := time.Now()
	select {
	case <-ctx.Done():
		s.log.Printf("[SoT] LOAD CANCEL key=%q after %s: %v", key, time.Since(start), ctx.Err())
		return nil, ctx.Err()
	case <-time.After(s.latency):
	}

	var (
		payload []byte
		err     error
	)
	switch {
	case key == "chart:global":
		payload, err = s.buildChart("global", "Global Trending Top 10", seedTracks)
	case strings.HasPrefix(key, "chart:genre:"):
		g := strings.TrimPrefix(key, "chart:genre:")
		list := tracksByGenre(g)
		if len(list) == 0 {
			s.log.Printf("[SoT] LOAD NOT_FOUND key=%q (unknown genre)", key)
			return nil, datasource.ErrNotFound
		}
		payload, err = s.buildChart(g, "Trending · "+strings.ToUpper(g), list)
	case strings.HasPrefix(key, "track:"):
		id := strings.TrimPrefix(key, "track:")
		t, ok := trackByID(id)
		if !ok {
			s.log.Printf("[SoT] LOAD NOT_FOUND key=%q", key)
			return nil, datasource.ErrNotFound
		}
		payload, err = json.Marshal(t)
	default:
		s.log.Printf("[SoT] LOAD NOT_FOUND key=%q (unknown shape)", key)
		return nil, datasource.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// annotate load cost for charts
	if strings.HasPrefix(key, "chart:") {
		var ch Chart
		if json.Unmarshal(payload, &ch) == nil {
			ch.LoadMs = time.Since(start).Milliseconds()
			ch.Source = "chart-aggregator-v1"
			payload, _ = json.Marshal(ch)
		}
	}

	s.log.Printf("[SoT] LOAD ok key=%q bytes=%d elapsed=%s in_flight_now=%d",
		key, len(payload), time.Since(start), s.inFlight.Load())
	return payload, nil
}

func (s *ChartSource) buildChart(board, title string, tracks []Track) ([]byte, error) {
	gen := s.generation.Load()
	type scored struct {
		t Track
		s float64
	}
	rows := make([]scored, 0, len(tracks))
	for i, t := range tracks {
		// deterministic-ish score from id + generation (stable within a gen)
		h := float64(0)
		for _, c := range t.ID {
			h += float64(c)
		}
		score := 1000 - float64(i)*3 + math.Mod(h*float64(gen+1)*17, 200)
		rows = append(rows, scored{t: t, s: score})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].s > rows[j].s })
	if len(rows) > 10 {
		rows = rows[:10]
	}
	ch := Chart{
		Board:     board,
		Title:     title,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		Entries:   make([]ChartEntry, 0, len(rows)),
	}
	sparks := []string{"▁▂▄▆█", "▂▄█▆▄", "▄█▄▂▁", "█▆▄▂▁", "▂▃▅▇█"}
	for i, r := range rows {
		ch.Entries = append(ch.Entries, ChartEntry{
			Rank:  i + 1,
			Track: r.t,
			Score: math.Round(r.s*10) / 10,
			Delta: int(math.Mod(hDelta(r.t.ID, gen), 7)) - 3,
			Spark: sparks[i%len(sparks)],
		})
	}
	return json.Marshal(ch)
}

func hDelta(id string, gen int64) float64 {
	var h float64
	for _, c := range id {
		h += float64(c)
	}
	return math.Mod(h*float64(gen+3), 11)
}
