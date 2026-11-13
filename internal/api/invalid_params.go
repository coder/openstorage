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
// problem answers with that problem unchanged.
type bodyErrors []error

// add collects err. A nil err is no problem.
func (b *bodyErrors) add(err error) {
	if err != nil {
		*b = append(*b, err)
	}
}

// err returns nil for a body without a problem, and the only problem
// itself for a body with one. For several problems it returns one error:
// the message of each problem in order, joined by "; ", and one
// invalid_params item for each field they name. A problem that names no
// field adds its message only. A field that two problems name counts once.
// After maxInvalidParams items, the message says how many problems it
// left out.
func (b bodyErrors) err() error {
	switch len(b) {
	case 0:
		return nil
	case 1:
		return b[0]
	}
	var (
		params []invalidParam
		msgs   []string
		named  = make(map[string]bool)
		more   int
	)
	for i, e := range b {
		if len(params) >= maxInvalidParams || len(msgs) >= maxInvalidParams {
			more = len(b) - i
			break
		}
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
	params = params[:min(len(params), maxInvalidParams)]
	if more > 0 {
		msgs = append(msgs, fmt.Sprintf("and %d more", more))
	}
	msg := strings.Join(msgs, "; ")
	if len(params) == 0 {
		return errors.New(msg)
	}
	return newFieldErrors(msg, params...)
}
