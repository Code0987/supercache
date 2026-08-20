// Example: fixed-window rate limiter on ModeCounter.
//
//	go run ./examples/ratelimit
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
