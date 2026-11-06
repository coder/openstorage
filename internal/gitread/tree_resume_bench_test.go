package gitread

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// BenchmarkLsTreePageDeepCursor pages trees of 100,000 files with limit
// 1000. "stream" is the page that reads the listing from its start up to the
// page, so it grows with the entries before the cursor, and "resume" is
// LsTreePage, which reads the folders on the cursor's path and the ones after
// it (#615). The shapes:
//
//   - hundreds: folders of 100 files, ten folders in each top-level folder,
//     as in the #612 and #615 measurements
//   - one folder: all files in one folder, which a cursor in it reads as far as
//     the cursor
//   - tens: folders of ten files
//   - one file each: a folder for every file, which makes the walk open a
//     folder for every file of the page
//   - chain: folders of one file and one folder, a hundred levels deep
//
// The cursors: the second page, the tenth, the middle and near the end. A
// shallow cursor is where the stream is cheap, and the walk must not cost
// much more than it. cpu-ms/op is the CPU time of the process and its git
// children per call: on a busy host it moves less than ns/op.
func BenchmarkLsTreePageDeepCursor(b *testing.B) {
	const files, limit = 100_000, 1000
	for _, shape := range []struct {
		name string
		path func(i int) string
	}{
		{"hundreds", func(i int) string { return fmt.Sprintf("t%04d/s%d/f%02d.txt", i/1000, i/100%10, i%100) }},
		{"one folder", func(i int) string { return fmt.Sprintf("d/f%06d.txt", i) }},
		{"tens", func(i int) string { return fmt.Sprintf("t%04d/p%02d/f%d.txt", i/1000, i/10%100, i%10) }},
		{"one file each", func(i int) string { return fmt.Sprintf("p%06d/f.txt", i) }},
		{"chain", func(i int) string {
			return fmt.Sprintf("c%04d/", i/100) + strings.Repeat("x/", i%100) + "f.txt"
		}},
	} {
		var in strings.Builder
		in.WriteString("commit refs/heads/main\ncommitter C <c@example.com> 1700000000 +0000\ndata 0\nM 100644 inline README.md\ndata 6\nhello\n")
		for i := range files {
			fmt.Fprintf(&in, "M 100644 inline %s\ndata %d\n%d\n", shape.path(i), len(fmt.Sprint(i)), i)
		}
		dir, commit := pageRepo(b, in.String()+"\n")
		for _, c := range []struct {
			name  string
			first int // index of the first file of the page
		}{{"first page", 0}, {"second page", limit}, {"tenth page", 9 * limit}, {"middle", files / 2}, {"near the end", files - 1500}} {
			after, want := "", "README.md"
			if c.first > 0 {
				after, want = shape.path(c.first-1), shape.path(c.first)
			}
			for _, impl := range []string{"stream", "resume"} {
				b.Run(shape.name+"/"+c.name+"/"+impl, func(b *testing.B) {
					cpu := cpuTime()
					for b.Loop() {
						var page []TreeEntry
						var more, found bool
						var err error
						if impl == "stream" {
							page, more, found, err = streamTreePage(context.Background(), dir, commit, "", after, limit, nil)
						} else {
							page, more, found, err = LsTreePage(context.Background(), dir, commit, "", after, limit)
						}
						if err != nil || !found || len(page) != limit || page[0].Path != want || !more {
							b.Fatalf("%d entries from %q (want %q), more %v, found %v, err %v", len(page), firstPath(page), want, more, found, err)
						}
					}
					if cpu > 0 {
						b.ReportMetric(float64((cpuTime()-cpu).Microseconds())/float64(b.N)/1000, "cpu-ms/op")
					}
				})
			}
		}
	}
}

func firstPath(page []TreeEntry) string {
	if len(page) == 0 {
		return ""
	}
	return page[0].Path
}
