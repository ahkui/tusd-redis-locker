package redislocker

import (
	"context"
	"errors"

	"github.com/go-redsync/redsync/v4"
	"github.com/tus/tusd/v2/pkg/handler"
)

// ErrLockNotHeld is returned by Unlock when the lock is not currently held
// by this instance.
var ErrLockNotHeld = errors.New("redislocker: lock not held")

// redisLock implements handler.Lock backed by a redsync.Mutex.
type redisLock struct {
	mutex *redsync.Mutex

	isHeld bool
}

func newRedisLock(mutex *redsync.Mutex) *redisLock {
	return &redisLock{mutex: mutex}
}

// Lock attempts to acquire the distributed lock, blocking (subject to the
// given context) until it succeeds or the context is done.
//
// Because the lock is coordinated through Redis across potentially many
// separate processes, there is no reliable transport for actively notifying
// the current holder that another caller wants the lock (unlike in-memory or
// single-host file lockers). We still invoke requestRelease as a best-effort
// signal for holders running in the same process, then fall back to
// redsync's built-in retry-with-backoff loop, which itself respects ctx
// cancellation.
func (lock *redisLock) Lock(ctx context.Context, requestRelease func()) error {
	if lock.isHeld {
		return handler.ErrFileLocked
	}

	// Fast path: try to acquire the lock immediately without waiting.
	err := lock.mutex.TryLockContext(ctx)
	if err == nil {
		lock.isHeld = true
		return nil
	}

	// The lock is currently held by someone else. Signal local holders (if
	// any) that we would like the lock to be released, then retry until the
	// context is done.
	requestRelease()

	err = lock.mutex.LockContext(ctx)
	if err != nil {
		// Any failure to acquire the lock within the retry budget (context
		// cancellation, exhausted retries, or the lock still being held by
		// another party) is surfaced as ErrLockTimeout, matching the
		// semantics expected by tusd's other Locker implementations.
		return handler.ErrLockTimeout
	}

	lock.isHeld = true
	return nil
}

// Unlock releases the distributed lock.
func (lock *redisLock) Unlock() error {
	if !lock.isHeld {
		return ErrLockNotHeld
	}

	lock.isHeld = false

	ok, err := lock.mutex.UnlockContext(context.Background())
	if err != nil {
		return err
	}
	if !ok {
		return ErrLockNotHeld
	}

	return nil
}
