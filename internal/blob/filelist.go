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
	"syscall"
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
// read what it passed. The walk holds one directory's entries at a time per
// level of the path, as filepath.WalkDir, which fileblob uses, does.
func walkFileKeys(ctx context.Context, dir, keyPrefix, prefix, after string, visit func(key string, de fs.DirEntry) error) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			return nil // removed while the walk ran, or a file where the prefix names a directory
		}
		return fmt.Errorf("list %s: %w", dir, err)
	}
	// Sort the directory in place, by the key of each entry. The sort key of a
	// directory is its key plus "/". Only the directories allocate for it.
	keyOf := func(de fs.DirEntry) string {
		key := unescapeFileName(de.Name())
		if de.IsDir() {
			key += "/"
		}
		return key
	}
	slices.SortFunc(entries, func(a, b fs.DirEntry) int { return strings.Compare(keyOf(a), keyOf(b)) })
	for _, de := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		name := de.Name()
		if strings.HasSuffix(name, fileblobAttrsExt) {
			continue
		}
		key := keyPrefix + keyOf(de)
		if !de.IsDir() {
			if strings.HasPrefix(key, prefix) && key > after {
				if err := visit(key, de); err != nil {
					return err
				}
			}
			continue
		}
		// Every key under the directory starts with key, which ends in "/".
		if !strings.HasPrefix(key, prefix) && !strings.HasPrefix(prefix, key) {
			continue
		}
		// after is past every key under the directory when it sorts at or above
		// "<dir>/" without starting with it: it is then at or above "<dir>0".
		if after >= key && !strings.HasPrefix(after, key) {
			continue
		}
		if err := walkFileKeys(ctx, filepath.Join(dir, name), key, prefix, after, visit); err != nil {
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
	// Start in the deepest directory that the prefix names, as fileblob does, so
	// that a listing of one repository does not read the directory of all of
	// them. A prefix whose directories fileblob would escape starts at the root.
	keyPrefix := ""
	if i := strings.LastIndex(prefix, "/"); i > 0 && plainDirPath(prefix[:i]) {
		dir = filepath.Join(dir, filepath.FromSlash(prefix[:i]))
		keyPrefix = prefix[:i+1]
	}
	page := make([]ListedObject, 0, pageSize)
	err := walkFileKeys(ctx, dir, keyPrefix, prefix, after, func(key string, de fs.DirEntry) error {
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

// plainDirPath reports that every directory of path is stored under its own
// name: fileblob escapes control characters, a slash after ".." or "/", and
// anything that looks like its own "__0x<hex>__" escape, and this package does
// not reproduce the escaped spelling of a directory.
func plainDirPath(path string) bool {
	for _, part := range strings.Split(path, "/") {
		if part == "" || part == "." || part == ".." || strings.Contains(part, "__") {
			return false
		}
		for i := 0; i < len(part); i++ {
			if part[i] < 0x20 || part[i] > 0x7e {
				return false
			}
		}
	}
	return true
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
