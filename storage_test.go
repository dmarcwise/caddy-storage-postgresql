package caddypostgresql

import (
	"context"
	"errors"
	"testing"
	"time"

	"cirello.io/pglock"
	"github.com/caddyserver/caddy/v2"
	"github.com/google/uuid"
)

func createStorage(t *testing.T) *PostgresStorage {
	t.Helper()

	s := NewPostgresStorage()
	s.ConnectionString = "postgres://postgres:@localhost:5432/caddy_tests"
	s.InstanceId = uuid.New().String()

	ctx, _ := caddy.NewContext(caddy.Context{Context: context.Background()})
	if err := s.Provision(ctx); err != nil {
		t.Fatalf("PostgreSQL unavailable or misconfigured: %v", err)
	}

	t.Cleanup(func() { _ = s.Cleanup() })

	return s
}

func Test_Lock_DifferentNamesDoNotBlock(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	nameA := t.Name() + ":a"
	nameB := t.Name() + ":b"

	if err := s.Lock(ctx, nameA); err != nil {
		t.Fatalf("lock A failed: %v", err)
	}

	if err := s.Lock(ctx, nameB); err != nil {
		t.Fatalf("lock B failed: %v", err)
	}
}

func Test_Lock_CrossClientBlocking(t *testing.T) {
	s1 := createStorage(t)
	s2 := createStorage(t)
	ctx := t.Context()
	name := t.Name()

	if err := s1.Lock(ctx, name); err != nil {
		t.Fatalf("s1 first lock failed: %v", err)
	}

	// s2 should not be able to acquire within a short timeout while s1 holds it
	errCh := make(chan error, 1)
	go func() {
		wctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		errCh <- s2.Lock(wctx, name)
	}()
	if err := <-errCh; !errors.Is(err, pglock.ErrNotAcquired) {
		t.Fatalf("expected s2 lock timeout while s1 holds; got %v", err)
	}

	// After releasing on s1, s2 should be able to acquire.
	if err := s1.Unlock(ctx, name); err != nil {
		t.Fatalf("s1 unlock failed: %v", err)
	}

	wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := s2.Lock(wctx, name); err != nil {
		t.Fatalf("s2 lock after release failed: %v", err)
	}
}

func Test_UnlockWithoutPriorLock(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()
	name := t.Name()

	if err := s.Unlock(ctx, name); err == nil {
		t.Fatalf("expected error on unlock without prior lock")
	}
}

func Test_Lock_ContextCanceled(t *testing.T) {
	s := createStorage(t)
	name := t.Name()
	cctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := s.Lock(cctx, name); !errors.Is(err, pglock.ErrNotAcquired) {
		t.Fatalf("expected context cancellation error; got %v", err)
	}
}
