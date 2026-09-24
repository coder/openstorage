package main

import (
	"testing"

	"github.com/coder/openstorage/internal/metrics"
)

// TestMetricsSurface pins the mux-pattern → surface mapping (ADR-48): the
// three registrations in serve, plus the redirect-target paths the mux
// reports for its own redirects.
func TestMetricsSurface(t *testing.T) {
	for pattern, want := range map[string]metrics.Surface{
		"/api/":               metrics.SurfaceAPI,
		"/api/v1/repos":       metrics.SurfaceAPI, // path-cleaning redirect target
		"/healthz":            metrics.SurfaceHealth,
		"/":                   metrics.SurfaceGit,
		"/repo.git/info/refs": metrics.SurfaceGit,
		"":                    metrics.SurfaceGit,
		"/api":                metrics.SurfaceGit, // not the /api/ subtree; the mux redirects it and reports "/api/"
	} {
		if got := metricsSurface(pattern); got != want {
			t.Errorf("metricsSurface(%q) = %v, want %v", pattern, got, want)
		}
	}
}
