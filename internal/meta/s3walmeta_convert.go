// S3WAL metadata format conversion (ADR-51): the only code that reads and
// writes a `_wal/meta.json` version other than s3walMetaVersion. The caller
// fences every other writer first (`openstorage s3wal-meta convert` holds
// the instance lease while serve is stopped). Correctness against a writer
// the fence missed still holds: every write is an If-Match CAS on the
// version this conversion read, so a stale writer loses with 412 and then
// fails to decode the converted document.
package meta

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/coder/openstorage/internal/ref/s3wal"
)

// S3WALMetaCounts are the row counts of one metadata document.
type S3WALMetaCounts struct {
	Orgs, Repos, Webhooks, Credentials, Upstreams int
}

// S3WALMetaConversion reports one ConvertS3WALMeta call.
type S3WALMetaConversion struct {
	// Missing is true when the bucket has no metadata document. Nothing
	// is written: the first write of the serving binary creates one.
	Missing bool
	// From and To are the stored versions before and after. From == To
	// means the document already had the target version (no write).
	From, To int
	// SeqBefore and SeqAfter are the stored document sequence numbers.
	SeqBefore, SeqAfter int64
	// Before and After count the rows of the source and the result.
	Before, After S3WALMetaCounts
}

// s3walConvertAttempts bounds lost-race retries, as mutate does.
const s3walConvertAttempts = 16

// ConvertS3WALMeta rewrites the bucket's metadata document to version to
// (1 or 2). It applies the zero-value rule of ADR-51: to 2 changes only
// the version; to 1 drops the four v2 fields and sets sync_pending on
// every lane whose pull claim is set, so v1 boot recovery of any release
// restarts that pull. After the write it re-reads the document and checks
// it equals the expected conversion of the source. Re-running on a
// document at the target version writes nothing.
func ConvertS3WALMeta(ctx context.Context, cs s3wal.ConditionalStore, to int) (S3WALMetaConversion, error) {
	assertf(cs != nil, "ConvertS3WALMeta: nil conditional store")
	if to != s3walMetaV1 && to != s3walMetaV2 {
		return S3WALMetaConversion{}, fmt.Errorf("s3walmeta convert: target version %d unsupported (want 1 or 2)", to)
	}
	for attempt := 0; attempt < s3walConvertAttempts; attempt++ {
		// The caller cancels ctx when its fence (the lease) is lost:
		// never start a write after that.
		if err := ctx.Err(); err != nil {
			return S3WALMetaConversion{}, fmt.Errorf("s3walmeta convert: %w", err)
		}
		obj, err := cs.Get(ctx, s3walMetaKey)
		if errors.Is(err, s3wal.ErrNotExist) {
			return S3WALMetaConversion{Missing: true, To: to}, nil
		}
		if err != nil {
			return S3WALMetaConversion{}, fmt.Errorf("s3walmeta convert: read: %w", err)
		}
		src, err := decodeS3WALMetaDocVersion(obj.Body, s3walMetaV1, s3walMetaV2)
		if err != nil {
			return S3WALMetaConversion{}, fmt.Errorf("s3walmeta convert: %w", err)
		}
		res := S3WALMetaConversion{From: src.V, To: to, SeqBefore: src.Seq, SeqAfter: src.Seq, Before: countS3WALMeta(src), After: countS3WALMeta(src)}
		if src.V == to {
			// Also reached when an earlier attempt's ambiguous PUT landed
			// and a second converter then advanced the document: the
			// result is right, but From == To understates our write.
			return res, nil
		}
		want, err := convertS3WALMetaDoc(obj.Body, to)
		if err != nil {
			return S3WALMetaConversion{}, err
		}
		body, err := want.encodeNext()
		if err != nil {
			return S3WALMetaConversion{}, fmt.Errorf("s3walmeta convert: %w", err)
		}
		if err := ctx.Err(); err != nil { // the fence was lost during the read
			return S3WALMetaConversion{}, fmt.Errorf("s3walmeta convert: %w", err)
		}
		_, werr := cs.Advance(ctx, s3walMetaKey, body, obj.ETag)
		switch {
		case werr == nil:
		case errors.Is(werr, s3wal.ErrLostRace):
			// A writer the fence missed advanced first: convert its state.
			if err := sleepFullJitter(ctx, attempt, 25*time.Millisecond, 2*time.Second); err != nil {
				return S3WALMetaConversion{}, fmt.Errorf("s3walmeta convert: %w", err)
			}
			continue
		case errors.Is(werr, s3wal.ErrNotExecuted):
			continue // provably never sent
		default:
			if _, ok := errors.AsType[*s3wal.AmbiguousError](werr); !ok {
				return S3WALMetaConversion{}, fmt.Errorf("s3walmeta convert: write: %w", werr)
			}
			// Ambiguous: our write landed only if the stored writer
			// nonce is ours. Otherwise convert the fresh state again.
			fresh, rerr := cs.Get(ctx, s3walMetaKey)
			if rerr != nil {
				return S3WALMetaConversion{}, fmt.Errorf("s3walmeta convert: write: %w (recovery read failed: %v)", werr, rerr)
			}
			doc, derr := decodeS3WALMetaDocVersion(fresh.Body, s3walMetaV1, s3walMetaV2)
			if derr != nil {
				return S3WALMetaConversion{}, fmt.Errorf("s3walmeta convert: recovery read: %w", derr)
			}
			if doc.Writer != want.Writer {
				continue
			}
		}
		if err := verifyS3WALMetaConversion(ctx, cs, body, want); err != nil {
			return S3WALMetaConversion{}, err
		}
		res.SeqAfter, res.After = want.Seq, countS3WALMeta(want)
		return res, nil
	}
	return S3WALMetaConversion{}, fmt.Errorf("s3walmeta convert: %d attempts: %w", s3walConvertAttempts, s3wal.ErrCASContention)
}

// convertS3WALMetaDoc decodes body (v1 or v2) fresh and applies the ADR-51
// conversion to version to. Decoding fresh keeps the result from aliasing
// any snapshot the caller compares against.
func convertS3WALMetaDoc(body []byte, to int) (*s3walMetaDoc, error) {
	doc, err := decodeS3WALMetaDocVersion(body, s3walMetaV1, s3walMetaV2)
	if err != nil {
		return nil, fmt.Errorf("s3walmeta convert: %w", err)
	}
	doc.V = to
	if to == s3walMetaV1 {
		for _, r := range doc.Repos {
			r.CreateState = 0
		}
		for _, u := range doc.Upstreams {
			u.PushAttempt, u.PushDue, u.SyncDue = 0, 0, 0
			// A pull claim consumed sync_pending. v1 releases before
			// #427 restart only marked lanes at boot, so re-mark it
			// here. The push lane needs nothing: every v1 release lists
			// push_syncing lanes at boot.
			if u.Syncing {
				u.SyncPending = true
			}
		}
	}
	return doc, nil
}

// verifyS3WALMetaConversion re-reads the stored document and checks it is
// exactly what we wrote: body, decoded back. Comparing decoded bodies (not
// want itself) keeps an empty source map, which the encoder omits, from
// reading as a mismatch.
func verifyS3WALMetaConversion(ctx context.Context, cs s3wal.ConditionalStore, body []byte, want *s3walMetaDoc) error {
	obj, err := cs.Get(ctx, s3walMetaKey)
	if err != nil {
		return fmt.Errorf("s3walmeta convert: verify read: %w", err)
	}
	got, err := decodeS3WALMetaDocVersion(obj.Body, want.V)
	if err != nil {
		return fmt.Errorf("s3walmeta convert: verify: %w", err)
	}
	if got.Writer != want.Writer || got.Seq != want.Seq {
		return fmt.Errorf("s3walmeta convert: verify: document advanced to seq %d by writer %s after the conversion wrote seq %d; another writer is running", got.Seq, got.Writer, want.Seq)
	}
	wrote, err := decodeS3WALMetaDocVersion(body, want.V)
	if err != nil {
		return fmt.Errorf("s3walmeta convert: verify: %w", err)
	}
	if !reflect.DeepEqual(got, wrote) {
		return fmt.Errorf("s3walmeta convert: verify: stored seq %d does not decode to the expected document", got.Seq)
	}
	return nil
}

func countS3WALMeta(doc *s3walMetaDoc) S3WALMetaCounts {
	return S3WALMetaCounts{
		Orgs: len(doc.Orgs), Repos: len(doc.Repos), Webhooks: len(doc.Webhooks),
		Credentials: len(doc.Credentials), Upstreams: len(doc.Upstreams),
	}
}
