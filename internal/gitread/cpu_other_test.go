//go:build !unix

package gitread

import "time"

func cpuTime() time.Duration { return 0 }
