package sync

import (
	"strings"
	"testing"
)

func TestParsePushRejections(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []RejectedRef
	}{
		{
			name: "non-fast-forward",
			in: "To file:///up/own/proj.git\n" +
				"!\trefs/heads/main:refs/heads/main\t[rejected] (non-fast-forward)\n" +
				"Done\n",
			want: []RejectedRef{{Ref: "refs/heads/main", Reason: "non-fast-forward"}},
		},
		{
			name: "remote rejected with server reason",
			in:   "!\trefs/heads/x:refs/heads/x\t[remote rejected] (pre-receive hook declined)\n",
			want: []RejectedRef{{Ref: "refs/heads/x", Reason: "pre-receive hook declined"}},
		},
		{
			name: "mixed outcomes keep only rejections",
			in: "To https://github.com/own/proj.git\n" +
				" \trefs/heads/ok:refs/heads/ok\tabc123..def456\n" +
				"*\trefs/tags/v2:refs/tags/v2\t[new tag]\n" +
				"!\trefs/tags/v1:refs/tags/v1\t[rejected] (already exists)\n" +
				"Done\n",
			want: []RejectedRef{{Ref: "refs/tags/v1", Reason: "already exists"}},
		},
		{
			name: "summary without parenthetical falls back verbatim",
			in:   "!\trefs/heads/y:refs/heads/y\t[remote rejected]\n",
			want: []RejectedRef{{Ref: "refs/heads/y", Reason: "[remote rejected]"}},
		},
		{name: "clean push", in: "To x\n \trefs/heads/main:refs/heads/main\tabc..def\nDone\n", want: nil},
		{name: "empty", in: "", want: nil},
		{name: "garbage", in: "!\n!\t\n!\tnofields\n!\t:\tx\n", want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePushRejections(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("entry %d = %+v, want %+v", i, got[i], tc.want[i])
				}
			}
		})
	}
}

func TestParsePushRejectionsBounds(t *testing.T) {
	line := "!\trefs/heads/b:refs/heads/b\t[remote rejected] (" + strings.Repeat("x", 5000) + ")\n"
	got := parsePushRejections(strings.Repeat(line, maxRejectedRefs*3))
	if len(got) != maxRejectedRefs {
		t.Fatalf("entry count = %d, want capped at %d", len(got), maxRejectedRefs)
	}
	for _, r := range got {
		if len(r.Reason) > maxRejectedReasonLen {
			t.Fatalf("reason length %d exceeds cap %d", len(r.Reason), maxRejectedReasonLen)
		}
	}
}

// FuzzParsePushRejections: the parser consumes git porcelain output whose
// "remote rejected" reasons quote upstream-controlled text (issue #90). It
// must never panic and must respect its output bounds — the entries flow
// into webhook event payloads.
func FuzzParsePushRejections(f *testing.F) {
	f.Add("To file:///up.git\n!\trefs/heads/main:refs/heads/main\t[rejected] (non-fast-forward)\nDone\n")
	f.Add("!\trefs/heads/x:refs/heads/x\t[remote rejected] (hook declined)\n")
	f.Add("!\t:\t()\n!\ta:b\t \n")
	f.Add(strings.Repeat("!\tr:r\t[rejected] (x)\n", 200))
	f.Fuzz(func(t *testing.T, porcelain string) {
		got := parsePushRejections(porcelain)
		if len(got) > maxRejectedRefs {
			t.Fatalf("entry count %d exceeds cap %d", len(got), maxRejectedRefs)
		}
		for _, r := range got {
			if r.Ref == "" || r.Reason == "" {
				t.Fatalf("empty field in %+v", r)
			}
			if len(r.Reason) > maxRejectedReasonLen {
				t.Fatalf("reason length %d exceeds cap %d", len(r.Reason), maxRejectedReasonLen)
			}
		}
	})
}
