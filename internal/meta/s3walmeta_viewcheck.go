//go:build openstorage_viewcheck

package meta

// The openstorage_viewcheck build tag turns on the shared-view check
// (checkViewIntact) in any binary, so tests outside this package catch a
// write into the shared metadata view too (issue #497): the e2e serve
// binary that internal/githttp builds, and CI's in-process test runs.
// Release builds never set the tag.
func init() { checkSharedView = true }
