package blob

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// fileblobAttrsExt is the suffix of the sidecar files fileblob keeps next to a
// blob when it writes metadata. File opens its bucket with MetadataDontWrite,
// but a directory that an older binary wrote can hold them.
const fileblobAttrsExt = ".attrs"

// walkFileKeys visits the files under dir in key order, the order BlobStore
// documents for ListPages. A key sorts by its bytes, so a directory sorts as
// its name plus "/". fileblob's own listing walks in file-name order instead.
// The two differ when one directory name is a prefix of another followed by a
// byte below "/", such as "repo+ephemeral" next to "repo". fileblob's page
// token then makes the next page skip every key that the walk reaches after
// the token, so its listing loses keys once a prefix holds more than a page.
//
// Only keys that start with prefix and sort after after are visited. A
// directory whose keys cannot match is skipped, so a resumed listing does not
// read what it passed.
func walkFileKeys(ctx context.Context, dir, keyPrefix, prefix, after string, visit func(key string, de fs.DirEntry) error) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil // removed while the walk ran
		}
		return fmt.Errorf("list %s: %w", dir, err)
	}
	type entry struct {
		sortKey string // the key, plus "/" for a directory
		name    string // the name on disk
		de      fs.DirEntry
	}
	list := make([]entry, 0, len(entries))
	for _, de := range entries {
		name := de.Name()
		if strings.HasSuffix(name, fileblobAttrsExt) {
			continue
		}
		key := keyPrefix + unescapeFileName(name)
		if de.IsDir() {
			key += "/"
		}
		list = append(list, entry{sortKey: key, name: name, de: de})
	}
	slices.SortFunc(list, func(a, b entry) int { return strings.Compare(a.sortKey, b.sortKey) })
	for _, e := range list {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !e.de.IsDir() {
			if strings.HasPrefix(e.sortKey, prefix) && e.sortKey > after {
				if err := visit(e.sortKey, e.de); err != nil {
					return err
				}
			}
			continue
		}
		// Every key under the directory starts with sortKey.
		if !strings.HasPrefix(e.sortKey, prefix) && !strings.HasPrefix(prefix, e.sortKey) {
			continue
		}
		// after is past every key under the directory when it sorts at or above
		// "<dir>/" without starting with it: it is then at or above "<dir>0".
		if after >= e.sortKey && !strings.HasPrefix(after, e.sortKey) {
			continue
		}
		if err := walkFileKeys(ctx, filepath.Join(dir, e.name), e.sortKey, prefix, after, visit); err != nil {
			return err
		}
	}
	return nil
}

// listFilePages streams the objects under dir that start with prefix and sort
// after after, in key order, in pages of at most pageSize objects.
func listFilePages(ctx context.Context, dir, prefix, after string, pageSize int, fn func([]ListedObject) error) error {
	assertf(dir != "", "listFilePages(%q): empty directory", prefix)
	assertf(pageSize > 0, "listFilePages(%q): non-positive page size %d", prefix, pageSize)
	assertf(fn != nil, "listFilePages(%q): nil fn", prefix)
	page := make([]ListedObject, 0, pageSize)
	err := walkFileKeys(ctx, dir, "", prefix, after, func(key string, de fs.DirEntry) error {
		info, err := de.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil // removed while the walk ran
			}
			return fmt.Errorf("stat %s: %w", key, err)
		}
		page = append(page, ListedObject{Key: key, Size: info.Size(), ModTime: info.ModTime()})
		if len(page) < pageSize {
			return nil
		}
		err = fn(page)
		page = page[:0]
		return err
	})
	if err != nil {
		return err
	}
	if len(page) > 0 {
		return fn(page)
	}
	return nil
}

// unescapeFileName reverses the escaping that fileblob applies to a key to make
// a file name. It writes a rune that needs escaping as "__0x<hex>__" and parses
// the same spelling back. This mirrors gocloud.dev/internal/escape.HexUnescape,
// which a package outside gocloud cannot import.
func unescapeFileName(s string) string {
	if !strings.Contains(s, "__0x") {
		return s
	}
	runes := []rune(s)
	out := make([]rune, 0, len(runes))
	for i := 0; i < len(runes); i++ {
		if r, end, ok := parseFileEscape(runes, i); ok {
			out = append(out, r)
			i = end
			continue
		}
		out = append(out, runes[i])
	}
	return string(out)
}

// parseFileEscape parses "__0x<hex>__" at runes[i] and returns the rune and the
// index of the last rune of the sequence.
func parseFileEscape(runes []rune, i int) (r rune, end int, ok bool) {
	const head = "__0x"
	for _, c := range head {
		if i >= len(runes) || runes[i] != c {
			return 0, 0, false
		}
		i++
	}
	start := i
	for i < len(runes) && runes[i] != '_' {
		i++
	}
	if i+1 >= len(runes) || runes[i] != '_' || runes[i+1] != '_' {
		return 0, 0, false
	}
	v, err := strconv.ParseInt(string(runes[start:i]), 16, 32)
	if err != nil {
		return 0, 0, false
	}
	return rune(v), i + 1, true
}
