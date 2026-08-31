package topkx

import (
	"bytes"
	"fmt"
	"testing"
)

func trackID(i int) []byte {
	return []byte(fmt.Sprintf("t%03d", i))
}

func formatList(rows []Entry) string {
	var b bytes.Buffer
	for i, e := range rows {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s=%d", e.Item, e.Count)
	}
	return b.String()
}

// Locked billboard stream (design §honest chart). N=1220, K=10.
func ingestHonestStream(t *testing.T, tab *Table) {
	t.Helper()
	play := func(id, n int) {
		item := trackID(id)
		for i := 0; i < n; i++ {
			if err := tab.Add(item); err != nil {
				t.Fatalf("add %s: %v", item, err)
			}
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
	for id := 21; id <= 500; id++ {
		play(id, 1)
	}
}
