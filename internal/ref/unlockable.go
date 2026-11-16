package ref

import "strings"

// fileNameMax is the file-name limit of the cache's file system (Linux
// NAME_MAX), which git's files backend must meet for every part of a ref
// name.
const fileNameMax = 255

// Unlockable reports whether git can never lock the ref called name in a
// cache repository. git creates the lock file <ref path>.lock before it
// updates or deletes a loose ref, so the file name of each `/`-separated
// part must fit fileNameMax, and the last part also needs room for ".lock":
// any part over 255 bytes, or a last part over 250 bytes, cannot be locked.
// Such a ref makes every hydration of its repository fail, because git
// cannot write it into the cache.
//
// The rule is about name bytes only. It leaves out the limit on the whole
// lock path, which depends on where each node keeps its cache directory.
func Unlockable(name string) bool {
	for {
		part, rest, more := strings.Cut(name, "/")
		limit := fileNameMax
		if !more {
			limit -= len(".lock")
		}
		if len(part) > limit {
			return true
		}
		if !more {
			return false
		}
		name = rest
	}
}
