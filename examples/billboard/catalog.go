package main

// Track is a music track card on the billboard.
type Track struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	Artist string `json:"artist"`
	Genre  string `json:"genre"`
	BPM    int    `json:"bpm,omitempty"`
}

// ChartEntry is one row on a trending board.
type ChartEntry struct {
	Rank   int     `json:"rank"`
	Track  Track   `json:"track"`
	Score  float64 `json:"score"`
	Delta  int     `json:"delta"` // rank change vs previous snapshot
	Spark  string  `json:"spark"` // tiny sparkline for UI
}

// Chart is a full board payload stored in SuperCache.
type Chart struct {
	Board     string       `json:"board"`
	Title     string       `json:"title"`
	UpdatedAt string       `json:"updated_at"`
	Source    string       `json:"source"`
	Entries   []ChartEntry `json:"entries"`
	LoadMs    int64        `json:"load_ms,omitempty"` // SoT compute cost (filled by DataSource)
}

var genres = []string{"pop", "hiphop", "electronic", "rock", "rnb"}

// seedTracks is the mock catalog the chart aggregator ranks.
var seedTracks = []Track{
	{ID: "t01", Title: "Neon Skyline", Artist: "Luna Vox", Genre: "pop", BPM: 118},
	{ID: "t02", Title: "Concrete Rivers", Artist: "MC Harbor", Genre: "hiphop", BPM: 92},
	{ID: "t03", Title: "Afterglow Protocol", Artist: "Synthwave 9", Genre: "electronic", BPM: 128},
	{ID: "t04", Title: "Broken Amplifiers", Artist: "The Static", Genre: "rock", BPM: 140},
	{ID: "t05", Title: "Velvet Frequency", Artist: "Aria Noir", Genre: "rnb", BPM: 86},
	{ID: "t06", Title: "Paper Crowns", Artist: "Luna Vox", Genre: "pop", BPM: 102},
	{ID: "t07", Title: "Cipher Dreams", Artist: "Byte Poet", Genre: "hiphop", BPM: 95},
	{ID: "t08", Title: "Gridlock Heart", Artist: "Pulse District", Genre: "electronic", BPM: 124},
	{ID: "t09", Title: "Rust & Chrome", Artist: "Iron Lantern", Genre: "rock", BPM: 132},
	{ID: "t10", Title: "Moonlit Lobby", Artist: "Sable Keys", Genre: "rnb", BPM: 78},
	{ID: "t11", Title: "Satellite Crush", Artist: "Orbit Girls", Genre: "pop", BPM: 120},
	{ID: "t12", Title: "Late Train Bars", Artist: "MC Harbor", Genre: "hiphop", BPM: 88},
	{ID: "t13", Title: "Analog Ghosts", Artist: "Tape Deck", Genre: "electronic", BPM: 110},
	{ID: "t14", Title: "Thunder Alley", Artist: "The Static", Genre: "rock", BPM: 148},
	{ID: "t15", Title: "Honey Voltage", Artist: "Aria Noir", Genre: "rnb", BPM: 90},
	{ID: "t16", Title: "Firework Math", Artist: "Luna Vox", Genre: "pop", BPM: 126},
	{ID: "t17", Title: "Sidewalk Cipher", Artist: "Byte Poet", Genre: "hiphop", BPM: 97},
	{ID: "t18", Title: "Photon Traffic", Artist: "Synthwave 9", Genre: "electronic", BPM: 130},
	{ID: "t19", Title: "Glass Canyon", Artist: "Iron Lantern", Genre: "rock", BPM: 118},
	{ID: "t20", Title: "Slow Orbit", Artist: "Sable Keys", Genre: "rnb", BPM: 72},
}

func trackByID(id string) (Track, bool) {
	for _, t := range seedTracks {
		if t.ID == id {
			return t, true
		}
	}
	return Track{}, false
}

func tracksByGenre(genre string) []Track {
	var out []Track
	for _, t := range seedTracks {
		if t.Genre == genre {
			out = append(out, t)
		}
	}
	return out
}
