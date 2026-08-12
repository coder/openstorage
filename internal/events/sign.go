package events

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
)

// Sign computes the docs/06 delivery signature header value for one
// attempt: hex(HMAC-SHA256(secret, "<ts>." + body)) rendered as
// "t=<ts>,sha256=<hex>". Receivers reject timestamps older than ~300s, so
// callers must sign with the send-time timestamp on EVERY attempt
// (re-signing on retry); Deliverer.Attempt does exactly that.
func Sign(secret string, ts int64, body []byte) string {
	assertf(secret != "", "Sign: empty secret")
	assertf(ts > 0, "Sign: non-positive timestamp %d", ts)
	mac := hmac.New(sha256.New, []byte(secret))
	tsStr := strconv.FormatInt(ts, 10)
	mac.Write([]byte(tsStr))
	mac.Write([]byte("."))
	mac.Write(body)
	return "t=" + tsStr + ",sha256=" + hex.EncodeToString(mac.Sum(nil))
}
