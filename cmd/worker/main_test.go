package main

import (
	"net/http"
	"testing"
)

func TestWorkerWriteTimeoutCannotPreemptCoordinatorJobDeadline(t *testing.T) {
	server := newHTTPServer(http.NotFoundHandler())
	if server.WriteTimeout != 0 {
		t.Fatalf("WriteTimeout = %v, want disabled", server.WriteTimeout)
	}
}
