package main

import "testing"

func TestParseWorkersAcceptsOrderedPool(t *testing.T) {
	workers, err := parseWorkers([]string{
		"worker-a=http://127.0.0.1:8081",
		"worker-b=http://127.0.0.1:8082",
		"worker-c=http://127.0.0.1:8083",
	})
	if err != nil {
		t.Fatalf("parseWorkers() error = %v", err)
	}
	if len(workers) != 3 || workers[2].ID != "worker-c" {
		t.Fatalf("workers = %+v", workers)
	}
}

func TestParseWorkersRequiresActivePair(t *testing.T) {
	if _, err := parseWorkers([]string{"worker-a=http://127.0.0.1:8081"}); err == nil {
		t.Fatal("parseWorkers() accepted a one-worker pool")
	}
}
