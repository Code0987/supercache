// Example: ModeVectorSet cosine K-NN on a 3-node in-process cluster.
//
//	go run ./examples/vecset
package main

import (
	"fmt"
	"os"
)

func main() {
	if err := runDemo(os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "FAIL: %v\n", err)
		os.Exit(1)
	}
}
