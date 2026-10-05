package chaos

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (function roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func TestTransportDropsDeterministicallyAndReportsStats(t *testing.T) {
	transport, err := NewTransport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	}), Config{Seed: 7, DropRateBPS: 10_000})
	if err != nil {
		t.Fatalf("NewTransport() error = %v", err)
	}
	request, err := http.NewRequest(http.MethodPost, "http://127.0.0.1/test", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transport.RoundTrip(request); !errors.Is(err, ErrDropped) {
		t.Fatalf("RoundTrip() error = %v, want ErrDropped", err)
	}
	if stats := transport.Stats(); stats.Requests != 1 || stats.Drops != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestTransportHonorsContextDuringDelay(t *testing.T) {
	transport, err := NewTransport(roundTripperFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	}), Config{MaxDelay: time.Hour})
	if err != nil {
		t.Fatalf("NewTransport() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1/test", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transport.RoundTrip(request); !errors.Is(err, context.Canceled) {
		t.Fatalf("RoundTrip() error = %v, want context.Canceled", err)
	}
}
