package api

import (
	"errors"
	"fmt"
	"strings"
)

// maxInvalidParams bounds one answer. A body of 1 MiB can hold hundreds of
// thousands of bad array entries, and the 400 must not repeat them all.
const maxInvalidParams = 100

// bodyErrors collects the problems of one request body, so that its 400
// names every invalid field. Upstream's ValidationProblemDetails says
// `invalid_params` "names each invalid field".
//
// A validator adds one problem for each field and skips the later checks
// of a field that already failed. A route adds its fields in the order of
// its request body in docs/reference/http-api.md, and array entries by
// index, so the answer to one request never changes order. A body with one
// problem answers with that problem unchanged. The collector keeps at most
// maxInvalidParams problems and counts the rest, so a body full of bad
// array entries costs one answer of bounded size.
type bodyErrors struct {
	errs    []error
	dropped int
}

// add collects err. A nil err is no problem.
func (b *bodyErrors) add(err error) {
	switch {
	case err == nil:
	case len(b.errs) >= maxInvalidParams:
		b.dropped++
	default:
		b.errs = append(b.errs, err)
	}
}

// full reports whether the collector holds as many problems as it keeps.
// A loop over a body array checks it before it builds a problem, and calls
// skip instead when it is true, so that a body full of bad entries does not
// allocate one problem for each of them.
func (b *bodyErrors) full() bool { return len(b.errs) >= maxInvalidParams }

// skip counts a problem that the caller did not build because full is true.
func (b *bodyErrors) skip() { b.dropped++ }

// err returns nil for a body without a problem, and the only problem
// itself for a body with one. For several problems it returns one error:
// the message of each problem in order, joined by "; ", and one
// invalid_params item for each field they name. A problem that names no
// field adds its message only. A field that two problems name counts once.
// When the answer leaves problems out, the message ends with `and N more`.
func (b *bodyErrors) err() error {
	switch len(b.errs) + b.dropped {
	case 0:
		return nil
	case 1:
		if len(invalidParamsFor(b.errs[0])) <= maxInvalidParams {
			return b.errs[0]
		}
	}
	var (
		params []invalidParam
		msgs   []string
		named  = make(map[string]bool)
	)
	for _, e := range b.errs {
		items := invalidParamsFor(e)
		fresh := len(items) == 0
		for _, p := range items {
			if !named[p.Name] {
				named[p.Name] = true
				params = append(params, p)
				fresh = true
			}
		}
		if fresh {
			msgs = append(msgs, e.Error())
		}
	}
	more := b.dropped
	if len(params) > maxInvalidParams {
		more += len(params) - maxInvalidParams
		params = params[:maxInvalidParams]
	}
	if more > 0 {
		msgs = append(msgs, fmt.Sprintf("and %d more", more))
	}
	msg := strings.Join(msgs, "; ")
	if len(params) == 0 {
		return errors.New(msg)
	}
	return newFieldErrors(msg, params...)
}
