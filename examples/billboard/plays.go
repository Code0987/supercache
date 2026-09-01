package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Code0987/supercache/pkg/client"
)

const (
	playsKS      = "plays"
	playsCountKS = "plays-count"
	playsName    = "hot"
	playK        = 10
	playIDs      = 500
)

// playTrackID is t001..t500 (distinct from SoT seedTracks t01..t20).
func playTrackID(i int) string {
	return fmt.Sprintf("t%03d", i)
}

// playMeta joins a play id to a synthetic card (or unknown).
func playMeta(id string) Track {
	var n int
	if _, err := fmt.Sscanf(id, "t%d", &n); err != nil || n < 1 || n > playIDs {
		return Track{ID: id, Title: id, Artist: "unknown"}
	}
	return Track{
		ID:     id,
		Title:  fmt.Sprintf("Track %03d", n),
		Artist: fmt.Sprintf("Artist %d", (n-1)%20+1),
		Genre:  genres[(n-1)%len(genres)],
	}
}

// honestPlayStream is the locked head-then-tail dump (N=1220).
// t001×200, t002×150, t003×100, t004×80, t005×60, t006–t020×10, t021–t500×1.
func honestPlayStream() [][]byte {
	var out [][]byte
	play := func(id, n int) {
		item := []byte(playTrackID(id))
		for i := 0; i < n; i++ {
			out = append(out, item)
		}
	}
	play(1, 200)
	play(2, 150)
	play(3, 100)
	play(4, 80)
	play(5, 60)
	for id := 6; id <= 20; id++ {
		play(id, 10)
	}
	for id := 21; id <= playIDs; id++ {
		play(id, 1)
	}
	return out
}

// lockedHonestChart is the Space-Saving List() for honestPlayStream at K=10.
var lockedHonestChart = []struct {
	id    string
	count uint64
}{
	{"t001", 200},
	{"t002", 150},
	{"t495", 109},
	{"t496", 109},
	{"t497", 109},
	{"t498", 109},
	{"t499", 109},
	{"t500", 109},
	{"t493", 108},
	{"t494", 108},
}

// ingestHonestPlays is the scripted demo write path: one CMSIncr(1) per TopKAdd.
func (a *appServer) ingestHonestPlays(ctx context.Context, name string) error {
	cli := a.clients[0]
	for _, item := range honestPlayStream() {
		if err := cli.TopKAdd(ctx, playsKS, name, item); err != nil {
			return err
		}
		if err := cli.CMSIncr(ctx, playsCountKS, name, item, 1); err != nil {
			return err
		}
	}
	return nil
}

// handlePlays is the live ModeTopK board: /v1/plays/{name}
//
//	GET    → TopKList + catalog join
//	POST   → ?track=t001&n=100  n× TopKAdd (engine is +1 only; n is demo-only)
//	DELETE → Delete(name) tombstone
func (a *appServer) handlePlays(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/v1/plays/")
	name = strings.TrimSpace(name)
	if name == "" || strings.Contains(name, "/") {
		http.Error(w, "plays name required: /v1/plays/{name}", http.StatusBadRequest)
		return
	}
	cli, node := a.pick()
	nPlays := parsePlayN(r)
	timeout := 3 * time.Second
	if nPlays > 1 {
		timeout = 15 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	switch r.Method {
	case http.MethodGet:
		rows, ok, err := cli.TopKList(ctx, playsKS, name)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		if !ok {
			writeJSON(w, http.StatusNotFound, map[string]any{
				"present": false, "name": name, "via": node.ID,
			})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"present": true, "name": name, "via": node.ID,
			"entries": joinPlayRows(rows),
		})
	case http.MethodPost:
		track := strings.TrimSpace(r.URL.Query().Get("track"))
		if track == "" {
			b, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
			var body struct {
				Track string `json:"track"`
				N     int    `json:"n"`
			}
			if json.Unmarshal(b, &body) == nil {
				track = strings.TrimSpace(body.Track)
				if r.URL.Query().Get("n") == "" && body.N > 0 {
					nPlays = clampPlayN(body.N)
				}
			}
		}
		if track == "" {
			http.Error(w, "track required (?track= or JSON)", http.StatusBadRequest)
			return
		}
		item := []byte(track)
		if err := cli.CMSIncr(ctx, playsCountKS, name, item, uint64(nPlays)); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "added": 0})
			return
		}
		for i := 0; i < nPlays; i++ {
			if err := cli.TopKAdd(ctx, playsKS, name, item); err != nil {
				writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error(), "added": i})
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok": true, "name": name, "track": track, "n": nPlays, "via": node.ID,
		})
	case http.MethodDelete:
		errP := cli.Delete(ctx, playsKS, name)
		errC := cli.Delete(ctx, playsCountKS, name)
		if errP != nil || errC != nil {
			err := errP
			if err == nil {
				err = errC
			}
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "name": name, "via": node.ID})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleCounts is GET /v1/counts/{name}?track=t003 → CMSQuery.
func (a *appServer) handleCounts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	name := strings.TrimPrefix(r.URL.Path, "/v1/counts/")
	name = strings.TrimSpace(name)
	if name == "" || strings.Contains(name, "/") {
		http.Error(w, "counts name required: /v1/counts/{name}", http.StatusBadRequest)
		return
	}
	track := strings.TrimSpace(r.URL.Query().Get("track"))
	if track == "" {
		http.Error(w, "track required (?track=)", http.StatusBadRequest)
		return
	}
	cli, node := a.pick()
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	n, ok, err := cli.CMSQuery(ctx, playsCountKS, name, []byte(track))
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"present": false, "name": name, "via": node.ID,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"present": true, "name": name, "track": track, "count": n, "via": node.ID,
	})
}

func parsePlayN(r *http.Request) int {
	s := strings.TrimSpace(r.URL.Query().Get("n"))
	if s == "" {
		return 1
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 1
	}
	return clampPlayN(n)
}

func clampPlayN(n int) int {
	if n < 1 {
		return 1
	}
	if n > 500 {
		return 500
	}
	return n
}

func joinPlayRows(rows []client.TopKEntry) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for i, r := range rows {
		id := string(r.Item)
		meta := playMeta(id)
		out = append(out, map[string]any{
			"rank":   i + 1,
			"item":   id,
			"count":  r.Count,
			"title":  meta.Title,
			"artist": meta.Artist,
		})
	}
	return out
}
