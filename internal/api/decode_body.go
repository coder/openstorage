package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
)

// maxDecodeWork bounds the bytes that unmarshalBody decodes for one body
// (the sum over its decodes). Each decode and each search for the member
// to replace cost about 0.2 ms for each KiB of body, so the bound keeps one
// request under about 100 ms. A body of 2 KiB or less can name 100 wrong-type
// fields, a body of 10 KiB 26, and a body of more than 128 KiB only the
// first. The answer does not say that it stopped early.
const maxDecodeWork = 256 << 10

// unmarshalBody is json.Unmarshal for a request body that names every field
// of the wrong JSON type. encoding/json keeps decoding after a type error
// but returns only the first one. unmarshalBody replaces the member that
// failed by null, which every type accepts as no change, and decodes again
// until the body decodes or a bound is reached. A body with one wrong-type
// field returns the error of encoding/json unchanged. Any other error
// (syntax, a wrong top-level type) is returned unchanged.
func unmarshalBody(body []byte, dst any) error {
	err := json.Unmarshal(body, dst)
	te, ok := errors.AsType[*json.UnmarshalTypeError](err)
	if !ok || te.Field == "" {
		return err
	}
	var errs bodyErrors
	errs.add(te)
	work, budget := body, maxDecodeWork-len(body) // the first decode counts
	for !errs.full() && budget >= len(work) {
		next, ok := nullMembers(work, te.Field)
		if !ok {
			break
		}
		work, budget = next, budget-len(next)
		te, ok = errors.AsType[*json.UnmarshalTypeError](json.Unmarshal(work, dst))
		if !ok || te.Field == "" {
			break
		}
		errs.add(te)
	}
	return errs.err()
}

// nullMembers returns body with every value at the dotted path field
// (members by name, array elements by index) replaced by null. It reports
// false when no value is found at the path.
func nullMembers(body []byte, field string) ([]byte, bool) {
	spans := memberSpans(body, strings.Split(field, "."))
	if len(spans) == 0 {
		return nil, false
	}
	var out bytes.Buffer
	prev := 0
	for _, sp := range spans {
		out.Write(body[prev:sp[0]])
		out.WriteString("null")
		prev = sp[1]
	}
	out.Write(body[prev:])
	return out.Bytes(), true
}

// memberSpans returns the byte ranges of the values at path in the JSON
// value b. encoding/json matches a member name to a field without regard to
// case, and so does this.
func memberSpans(b []byte, path []string) [][2]int {
	dec := json.NewDecoder(bytes.NewReader(b))
	open, err := dec.Token()
	if err != nil {
		return nil
	}
	var spans [][2]int
	for index := 0; dec.More(); index++ {
		name := strconv.Itoa(index)
		if open == json.Delim('{') {
			key, err := dec.Token()
			if err != nil {
				return nil
			}
			name, _ = key.(string)
		}
		var raw json.RawMessage
		if dec.Decode(&raw) != nil {
			return nil
		}
		end := int(dec.InputOffset())
		start := end - len(raw)
		if !strings.EqualFold(name, path[0]) {
			continue
		}
		if len(path) == 1 {
			spans = append(spans, [2]int{start, end})
			continue
		}
		for _, sp := range memberSpans(raw, path[1:]) {
			spans = append(spans, [2]int{start + sp[0], start + sp[1]})
		}
	}
	return spans
}
