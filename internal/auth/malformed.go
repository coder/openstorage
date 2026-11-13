package auth

// MalformedError is a token, or a claim in it, that does not decode. A
// client reads Error, and it is fixed text: the text of a decoder names Go
// structs and types ("cannot unmarshal number into Go struct field
// Claims.exp of type int64"), so it stays in Err for the server's log.
type MalformedError struct {
	// What names the malformed part: "jwt", "jwt claims" or "refs claim".
	What string
	// Err is the decoder's error. Log it, never show it to a client.
	Err error
}

func (e *MalformedError) Error() string { return "malformed " + e.What }

// Unwrap exposes the decoder's error.
func (e *MalformedError) Unwrap() error { return e.Err }
