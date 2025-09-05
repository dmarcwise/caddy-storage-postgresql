package caddypostgresql

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"testing"
	"time"

	"cirello.io/pglock"
	"github.com/caddyserver/caddy/v2"
	"github.com/stretchr/testify/assert"
)

func createStorage(t *testing.T) *PostgresStorage {
	t.Helper()

	s := NewPostgresStorage()

	s.Dsn = os.Getenv("TESTS_CONNECTION_STRING")
	if s.Dsn == "" {
		s.Dsn = "postgres://postgres:@localhost:5432/caddy_tests"
	}

	ctx, _ := caddy.NewContext(caddy.Context{Context: context.Background()})
	if err := s.Provision(ctx); err != nil {
		t.Fatalf("PostgreSQL unavailable or misconfigured: %v", err)
	}

	// Start clean: empty the objects table
	_, err := s.pool.Exec(context.Background(), "DELETE FROM caddy_certmagic_objects")
	if err != nil {
		t.Errorf("objects cleanup failed: %v", err)
	}

	// And the locks table
	_, err = s.pool.Exec(context.Background(), "DELETE FROM caddy_locks")
	if err != nil {
		t.Errorf("locks cleanup failed: %v", err)
	}

	t.Cleanup(func() {
		_ = s.Cleanup()
	})

	return s
}

func Test_Lock_DifferentNamesDoNotBlock(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	nameA := t.Name() + ":a"
	nameB := t.Name() + ":b"

	err := s.Lock(ctx, nameA)
	assert.NoErrorf(t, err, "lock A failed")

	err = s.Lock(ctx, nameB)
	assert.NoErrorf(t, err, "lock B failed")
}

func Test_Lock_CrossClientBlocking(t *testing.T) {
	s1 := createStorage(t)
	s2 := createStorage(t)
	ctx := t.Context()
	name := t.Name()

	err := s1.Lock(ctx, name)
	assert.NoErrorf(t, err, "s1 first lock failed")

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
	err = s1.Unlock(ctx, name)
	assert.NoErrorf(t, err, "s1 unlock failed")

	wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	err = s2.Lock(wctx, name)
	assert.NoErrorf(t, err, "s2 lock after release failed")
}

func Test_UnlockMultipleTimes(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()
	name := t.Name()

	err := s.Lock(ctx, name)
	assert.NoErrorf(t, err, "first lock failed")

	err = s.Unlock(ctx, name)
	assert.NoErrorf(t, err, "first unlock failed")

	err = s.Unlock(ctx, name)
	assert.Errorf(t, err, "expected error on second unlock")
}

func Test_UnlockWithoutPriorLock(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()
	name := t.Name()

	err := s.Unlock(ctx, name)
	assert.Errorf(t, err, "expected error on unlock without prior lock")
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

func Test_Store_Simple(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	key := "file.txt"
	value := []byte("hello world")

	err := s.Store(ctx, key, value)
	assert.NoError(t, err)
}

func Test_Store_NestedDirectories(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	key := "a/b/c/file.txt"
	value := []byte("hello world")

	err := s.Store(ctx, key, value)
	assert.NoError(t, err)
}

func Test_Exists_Simple(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	key := "file.txt"
	value := []byte("hello world")

	exists := s.Exists(ctx, key)
	assert.False(t, exists)

	err := s.Store(ctx, key, value)
	assert.NoError(t, err)

	exists = s.Exists(ctx, key)
	assert.True(t, exists)
}

func Test_Exists_Directory(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	key := "a/b/c/file.txt"
	value := []byte("hello world")
	dir := "a/b/c"

	// Directory doesn't exist yet
	exists := s.Exists(ctx, dir)
	assert.False(t, exists)

	// File doesn't exist yet
	exists = s.Exists(ctx, key)
	assert.False(t, exists)

	err := s.Store(ctx, key, value)
	assert.NoError(t, err)

	// Now both should exist
	exists = s.Exists(ctx, key)
	assert.True(t, exists)

	exists = s.Exists(ctx, dir)
	assert.True(t, exists)
}

func Test_Load_Simple(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	key := "file.txt"
	value := []byte("hello world")

	err := s.Store(ctx, key, value)
	assert.NoError(t, err)

	loaded, err := s.Load(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value, loaded)
}

func Test_Load_Directory(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	key := "a/b/c/file.txt"
	value := []byte("hello world")

	err := s.Store(ctx, key, value)
	assert.NoError(t, err)

	loaded, err := s.Load(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, value, loaded)
}

func Test_Load_NonExistent(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	key := "no/such/file.txt"

	loaded, err := s.Load(ctx, key)
	assert.ErrorIs(t, err, fs.ErrNotExist)
	assert.Nil(t, loaded)
}

func Test_Stat_File(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	key := "file.txt"
	value := []byte("hello world")

	err := s.Store(ctx, key, value)
	assert.NoError(t, err)

	info, err := s.Stat(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, key, info.Key)
	assert.Equal(t, int64(11), info.Size)
	assert.WithinDuration(t, time.Now(), info.Modified, time.Second)
	assert.True(t, info.IsTerminal)
}

func Test_Stat_Directory(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	key := "a/b/c/file.txt"
	value := []byte("hello world")

	err := s.Store(ctx, key, value)
	assert.NoError(t, err)

	dir := "a/b/c"
	info, err := s.Stat(ctx, dir)
	assert.NoError(t, err)
	assert.Equal(t, dir, info.Key)
	assert.Equal(t, int64(0), info.Size)
	assert.WithinDuration(t, time.Now(), info.Modified, time.Second)
	assert.False(t, info.IsTerminal)

	dir = "a/b"
	info, err = s.Stat(ctx, dir)
	assert.NoError(t, err)
	assert.Equal(t, dir, info.Key)
	assert.Equal(t, int64(0), info.Size)
	assert.WithinDuration(t, time.Now(), info.Modified, time.Second)
	assert.False(t, info.IsTerminal)

	info, err = s.Stat(ctx, key)
	assert.NoError(t, err)
	assert.Equal(t, key, info.Key)
	assert.Equal(t, int64(11), info.Size)
	assert.WithinDuration(t, time.Now(), info.Modified, time.Second)
	assert.True(t, info.IsTerminal)
}

func Test_Stat_NonExistent(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	key := "no/such/file.txt"

	_, err := s.Stat(ctx, key)
	assert.ErrorIs(t, err, fs.ErrNotExist)
}

func Test_Delete_Simple(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	key := "file.txt"
	value := []byte("hello world")

	err := s.Store(ctx, key, value)
	assert.NoError(t, err)

	err = s.Delete(ctx, key)
	assert.NoError(t, err)

	exists := s.Exists(ctx, key)
	assert.False(t, exists)
}

func Test_Delete_Directory(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	key := "a/b/c/file.txt"
	value := []byte("hello world")

	err := s.Store(ctx, key, value)
	assert.NoError(t, err)

	key = "a/b/c/d/file2.txt"
	value = []byte("hello world 2")

	err = s.Store(ctx, key, value)
	assert.NoError(t, err)

	// Delete the directory
	dir := "a/b/c"
	err = s.Delete(ctx, dir)
	assert.NoError(t, err)

	// Both should be gone
	exists := s.Exists(ctx, key)
	assert.False(t, exists)
	exists = s.Exists(ctx, dir)
	assert.False(t, exists)
}

func Test_Delete_NonExistent(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	key := "no/such/file.txt"

	err := s.Delete(ctx, key)
	assert.NoError(t, err)
}

func createExampleStructure(t *testing.T, s *PostgresStorage, ctx context.Context) {
	// Example structure:
	// certificates/
	// ├── issuer1
	// │   └── domain1
	// │       ├── domain1.crt
	// │       └── domain1.key
	// └── issuer2
	//     ├── domain2
	//     │   ├── domain2.crt
	//     │   └── domain2.key
	//     └── domain3
	//         ├── domain3.crt
	//         └── domain3.key
	// accounts/
	// └── test1/
	//     └── account.json
	// instance.uuid
	// last_clean.json

	err := s.Store(ctx, "certificates/issuer1/domain1/domain1.crt", []byte("cert1"))
	assert.NoError(t, err)
	err = s.Store(ctx, "certificates/issuer1/domain1/domain1.key", []byte("key1"))
	assert.NoError(t, err)

	err = s.Store(ctx, "certificates/issuer2/domain2/domain2.crt", []byte("cert2"))
	assert.NoError(t, err)
	err = s.Store(ctx, "certificates/issuer2/domain2/domain2.key", []byte("key2"))
	assert.NoError(t, err)

	err = s.Store(ctx, "certificates/issuer2/domain3/domain3.crt", []byte("cert3"))
	assert.NoError(t, err)
	err = s.Store(ctx, "certificates/issuer2/domain3/domain3.key", []byte("key3"))
	assert.NoError(t, err)

	err = s.Store(ctx, "accounts/test1/account.json", []byte("{}"))
	assert.NoError(t, err)

	err = s.Store(ctx, "instance.uuid", []byte("some-uuid"))
	assert.NoError(t, err)

	err = s.Store(ctx, "last_clean.json", []byte("{}"))
	assert.NoError(t, err)
}

func Test_List_NonRecursive_Root(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	createExampleStructure(t, s, ctx)

	entries, err := s.List(ctx, "", false)
	assert.NoError(t, err)

	expectedKeys := []string{
		"certificates",
		"accounts",
		"instance.uuid",
		"last_clean.json",
	}

	assert.ElementsMatch(t, expectedKeys, entries)
}

func Test_List_NonRecursive(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	createExampleStructure(t, s, ctx)

	entries, err := s.List(ctx, "certificates", false)
	assert.NoError(t, err)

	assert.ElementsMatch(t, []string{
		"certificates/issuer1",
		"certificates/issuer2",
	}, entries)

	entries, err = s.List(ctx, "certificates/issuer1", false)
	assert.NoError(t, err)

	assert.ElementsMatch(t, []string{
		"certificates/issuer1/domain1",
	}, entries)

	entries, err = s.List(ctx, "certificates/issuer2", false)
	assert.NoError(t, err)

	assert.ElementsMatch(t, []string{
		"certificates/issuer2/domain2",
		"certificates/issuer2/domain3",
	}, entries)

	entries, err = s.List(ctx, "accounts", false)
	assert.NoError(t, err)

	assert.ElementsMatch(t, []string{
		"accounts/test1",
	}, entries)

	entries, err = s.List(ctx, "last_clean.json", false)
	assert.NoError(t, err)

	assert.ElementsMatch(t, []string{}, entries)
}

func Test_List_Recursive_Root(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	createExampleStructure(t, s, ctx)

	entries, err := s.List(ctx, "", true)
	assert.NoError(t, err)

	expectedKeys := []string{
		"certificates",
		"certificates/issuer1",
		"certificates/issuer1/domain1",
		"certificates/issuer1/domain1/domain1.crt",
		"certificates/issuer1/domain1/domain1.key",
		"certificates/issuer2",
		"certificates/issuer2/domain2",
		"certificates/issuer2/domain2/domain2.crt",
		"certificates/issuer2/domain2/domain2.key",
		"certificates/issuer2/domain3",
		"certificates/issuer2/domain3/domain3.crt",
		"certificates/issuer2/domain3/domain3.key",
		"accounts",
		"accounts/test1",
		"accounts/test1/account.json",
		"instance.uuid",
		"last_clean.json",
	}

	assert.ElementsMatch(t, expectedKeys, entries)
}

func Test_List_Recursive(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	createExampleStructure(t, s, ctx)

	entries, err := s.List(ctx, "certificates", true)
	assert.NoError(t, err)

	assert.ElementsMatch(t, []string{
		"certificates/issuer1",
		"certificates/issuer1/domain1",
		"certificates/issuer1/domain1/domain1.crt",
		"certificates/issuer1/domain1/domain1.key",
		"certificates/issuer2",
		"certificates/issuer2/domain2",
		"certificates/issuer2/domain2/domain2.crt",
		"certificates/issuer2/domain2/domain2.key",
		"certificates/issuer2/domain3",
		"certificates/issuer2/domain3/domain3.crt",
		"certificates/issuer2/domain3/domain3.key",
	}, entries)

	entries, err = s.List(ctx, "certificates/issuer2", true)
	assert.NoError(t, err)

	assert.ElementsMatch(t, []string{
		"certificates/issuer2/domain2",
		"certificates/issuer2/domain2/domain2.crt",
		"certificates/issuer2/domain2/domain2.key",
		"certificates/issuer2/domain3",
		"certificates/issuer2/domain3/domain3.crt",
		"certificates/issuer2/domain3/domain3.key",
	}, entries)
}

func Test_List_NonExistent(t *testing.T) {
	s := createStorage(t)
	ctx := t.Context()

	entries, err := s.List(ctx, "no/such/dir", false)
	assert.NoError(t, err)
	assert.Empty(t, entries)

	entries, err = s.List(ctx, "no/such/dir", true)
	assert.NoError(t, err)
	assert.Empty(t, entries)
}
