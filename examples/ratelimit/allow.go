package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Code0987/supercache/pkg/client"
)

// Allow is a fixed-window limiter: Incr(+1) on key:windowID; n > limit denies.
func Allow(ctx context.Context, cli *client.Client, ks, key string, limit int64, window time.Duration) (allowed bool, remaining, n int64, name string, err error) {
	secs := int64(window / time.Second)
	if secs < 1 {
		return false, 0, 0, "", fmt.Errorf("window must be >= 1s")
	}
	name = fmt.Sprintf("%s:%d", key, time.Now().Unix()/secs)
	n, err = cli.Incr(ctx, ks, name, 1)
	if err != nil {
		return false, 0, 0, name, err
	}
	if n > limit {
		return false, 0, n, name, nil
	}
	return true, limit - n, n, name, nil
}
