package redislocker

import (
	"context"
	"errors"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/redis/go-redis/v9"
	"github.com/tus/tusd/v2/pkg/handler"
)

// ErrLockNotHeld is returned by Unlock when the lock is not currently held
// by this instance.
var ErrLockNotHeld = errors.New("redislocker: lock not held")

// redisLock implements handler.Lock backed by a redsync.Mutex.
type redisLock struct {
	id     string
	mutex  *redsync.Mutex
	client redis.UniversalClient
	prefix string

	ctx    context.Context
	cancel context.CancelCauseFunc

	isHeld bool
}

func newRedisLock(id string, mutex *redsync.Mutex, client redis.UniversalClient, prefix string) *redisLock {
	return &redisLock{
		id:     id,
		mutex:  mutex,
		client: client,
		prefix: prefix,
	}
}

func (lock *redisLock) requestChannel() string {
	return lock.prefix + ":req:" + lock.id
}

func (lock *redisLock) releaseChannel() string {
	return lock.prefix + ":rel:" + lock.id
}

// Lock attempts to acquire the distributed lock, blocking (subject to the
// given context) until it succeeds or the context is done.
func (lock *redisLock) Lock(ctx context.Context, requestRelease func()) error {
	if lock.isHeld {
		return handler.ErrFileLocked
	}

	// Try to acquire the lock immediately without waiting.
	err := lock.acquire(ctx)
	if err == nil {
		lock.startKeepAliveAndListen(requestRelease)
		return nil
	}

	// The lock is currently held by someone else. Request them to release it.
	// We also call local requestRelease just in case the holder is local.
	if requestRelease != nil {
		requestRelease()
	}
	lock.client.Publish(ctx, lock.requestChannel(), lock.id)

	// Subscribe to release notifications from the current holder
	pubsub := lock.client.Subscribe(ctx, lock.releaseChannel())
	defer pubsub.Close()
	ch := pubsub.Channel()

	// Use a fallback ticker to periodically retry and re-request
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		err := lock.acquire(ctx)
		if err == nil {
			lock.startKeepAliveAndListen(requestRelease)
			return nil
		}

		select {
		case <-ctx.Done():
			return handler.ErrLockTimeout
		case _, ok := <-ch:
			if !ok {
				// Channel closed, stop listening to it and rely on the ticker
				ch = nil
			}
			// Notified of lock release (or closed), try to acquire immediately in next loop iteration
		case <-ticker.C:
			// Re-request release periodically just in case the original message was missed
			lock.client.Publish(ctx, lock.requestChannel(), lock.id)
		}
	}
}

func (lock *redisLock) acquire(ctx context.Context) error {
	if err := lock.mutex.TryLockContext(ctx); err != nil {
		return handler.ErrFileLocked
	}
	lock.isHeld = true
	lock.ctx, lock.cancel = context.WithCancelCause(context.Background())
	return nil
}

func (lock *redisLock) startKeepAliveAndListen(requestRelease func()) {
	// 1. Listen for requests from other instances to release the lock
	go func() {
		pubsub := lock.client.Subscribe(lock.ctx, lock.requestChannel())
		defer pubsub.Close()
		ch := pubsub.Channel()
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					return // Channel closed (e.g., Redis connection dropped)
				}
				if requestRelease != nil {
					requestRelease()
				}
			case <-lock.ctx.Done():
				return
			}
		}
	}()

	// 2. Keep the lock alive by periodically extending its TTL
	go func() {
		for {
			// Extend at half the TTL
			timeUntil := time.Until(lock.mutex.Until()) / 2
			if timeUntil <= 0 {
				timeUntil = time.Second // Fallback
			}

			select {
			case <-time.After(timeUntil):
				if _, err := lock.mutex.ExtendContext(lock.ctx); err != nil {
					// If we can't extend it (e.g. Redis is down or lock was lost),
					// cancel the lock's context and invoke release requested
					lock.cancel(err)
					if requestRelease != nil {
						requestRelease()
					}
					return
				}
			case <-lock.ctx.Done():
				return
			}
		}
	}()
}

// Unlock releases the distributed lock.
func (lock *redisLock) Unlock() error {
	if !lock.isHeld {
		return ErrLockNotHeld
	}

	lock.isHeld = false
	if lock.cancel != nil {
		lock.cancel(nil)
	}

	ok, err := lock.mutex.UnlockContext(context.Background())

	// Notify waiting instances that the lock is released
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	lock.client.Publish(ctx, lock.releaseChannel(), lock.id)

	if err != nil {
		return err
	}
	if !ok {
		return ErrLockNotHeld
	}

	return nil
}
