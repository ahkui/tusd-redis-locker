package redislocker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/tus/tusd/v2/pkg/handler"
)

var _ handler.Locker = &Locker{}

// newTestClient starts an in-process miniredis server and returns a
// redis.UniversalClient connected to it. The server is automatically
// stopped, and the client closed, when the test completes.
func newTestClient(t *testing.T) redis.UniversalClient {
	t.Helper()

	mr := miniredis.RunT(t)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() {
		_ = client.Close()
	})

	return client
}

func newTestLocker(t *testing.T, prefix string, ttl time.Duration) *Locker {
	t.Helper()

	client := newTestClient(t)

	locker, err := New(client, WithPrefix(prefix), WithTTL(ttl))
	if err != nil {
		t.Fatalf("failed to create redis locker: %v", err)
	}

	return locker
}

func TestRedisLocker_LockAndUnlock(t *testing.T) {
	a := assert.New(t)

	locker := newTestLocker(t, "test-tusd", 5*time.Second)

	lock1, err := locker.NewLock("one")
	a.NoError(err)

	a.NoError(lock1.Lock(context.Background(), func() {
		t.Fatal("must not be called")
	}))
	a.NoError(lock1.Unlock())
}

func TestRedisLocker_AlreadyLocked(t *testing.T) {
	a := assert.New(t)

	locker := newTestLocker(t, "test-tusd", 5*time.Second)

	lock1, err := locker.NewLock("one")
	a.NoError(err)
	a.NoError(lock1.Lock(context.Background(), func() {}))

	lock2, err := locker.NewLock("one")
	a.NoError(err)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	releaseRequested := false
	err = lock2.Lock(ctx, func() {
		releaseRequested = true
	})
	a.Equal(handler.ErrLockTimeout, err)
	a.True(releaseRequested)

	a.NoError(lock1.Unlock())
}

func TestRedisLocker_UnlockNotHeld(t *testing.T) {
	a := assert.New(t)

	locker := newTestLocker(t, "test-tusd", 5*time.Second)

	lock, err := locker.NewLock("one")
	a.NoError(err)
	a.Equal(ErrLockNotHeld, lock.Unlock())
}

func TestRedisLocker_ReleaseAllowsReacquire(t *testing.T) {
	a := assert.New(t)

	locker := newTestLocker(t, "test-tusd", 5*time.Second)

	lock1, err := locker.NewLock("one")
	a.NoError(err)
	a.NoError(lock1.Lock(context.Background(), func() {}))
	a.NoError(lock1.Unlock())

	lock2, err := locker.NewLock("one")
	a.NoError(err)
	a.NoError(lock2.Lock(context.Background(), func() {
		t.Fatal("must not be called, lock should be free")
	}))
	a.NoError(lock2.Unlock())
}

func TestRedisLocker_DifferentPrefixesAreIndependent(t *testing.T) {
	a := assert.New(t)

	// Both lockers share the same underlying miniredis instance, but use
	// different key prefixes, so their locks must not conflict.
	client := newTestClient(t)

	lockerA, err := New(client, WithPrefix("test-tusd-a"), WithTTL(5*time.Second))
	a.NoError(err)
	lockerB, err := New(client, WithPrefix("test-tusd-b"), WithTTL(5*time.Second))
	a.NoError(err)

	lockA, err := lockerA.NewLock("shared-id")
	a.NoError(err)
	a.NoError(lockA.Lock(context.Background(), func() {}))

	// Same upload ID, but a different locker prefix, so it must not
	// conflict with lockA's lock.
	lockB, err := lockerB.NewLock("shared-id")
	a.NoError(err)
	a.NoError(lockB.Lock(context.Background(), func() {
		t.Fatal("must not be called, different prefix means different key")
	}))

	a.NoError(lockA.Unlock())
	a.NoError(lockB.Unlock())
}

func TestNew_NilClient(t *testing.T) {
	a := assert.New(t)

	_, err := New(nil)
	a.Error(err)
}

func TestNew_WithFunctionalOptions(t *testing.T) {
	a := assert.New(t)

	client := newTestClient(t)

	prefix := fmt.Sprintf("test-tusd-opts-%d", time.Now().UnixNano())

	// New(client) with defaults.
	locker1, err := New(client)
	a.NoError(err)
	a.Equal(DefaultPrefix, locker1.options.Prefix())
	a.Equal(DefaultTTL, locker1.options.TTL())

	// New(client, WithPrefix(...))
	locker2, err := New(client, WithPrefix(prefix))
	a.NoError(err)
	a.Equal(prefix, locker2.options.Prefix())
	a.Equal(DefaultTTL, locker2.options.TTL())

	// New(client, WithPrefix(...), WithTTL(...))
	locker3, err := New(client, WithPrefix(prefix), WithTTL(15*time.Second))
	a.NoError(err)
	a.Equal(prefix, locker3.options.Prefix())
	a.Equal(15*time.Second, locker3.options.TTL())

	lock, err := locker3.NewLock("functional-options")
	a.NoError(err)
	a.NoError(lock.Lock(context.Background(), func() {
		t.Fatal("must not be called")
	}))
	a.NoError(lock.Unlock())
}
