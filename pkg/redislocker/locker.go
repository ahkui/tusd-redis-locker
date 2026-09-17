// Package redislocker provides a distributed locking mechanism for tusd
// backed by Redis, using redsync (https://github.com/go-redsync/redsync)
// for the distributed mutex algorithm and go-redis
// (https://github.com/redis/go-redis) as the Redis client.
//
// Locker is constructed from an existing redis.UniversalClient, which may
// be a *redis.Client (standalone/sentinel) or *redis.ClusterClient (Redis
// Cluster), so both deployment modes are supported by this package.
//
//	locker, err := redislocker.New(client)
//	locker, err := redislocker.New(client, redislocker.WithPrefix("my-prefix"))
//	locker, err := redislocker.New(client,
//		redislocker.WithPrefix("my-prefix"),
//		redislocker.WithTTL(15*time.Second),
//	)
//
// The locker needs to be registered with the composer used by tusd:
//
//	composer := handler.NewStoreComposer()
//	locker.UseIn(composer)
package redislocker

import (
	"fmt"

	"github.com/go-redsync/redsync/v4"
	goredis "github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	"github.com/tus/tusd/v2/pkg/handler"
)

// Locker is a handler.Locker implementation backed by Redis, using redsync
// to implement a distributed mutex per upload ID.
type Locker struct {
	client  redis.UniversalClient
	rs      *redsync.Redsync
	options options
}

// New constructs a Locker from an existing redis.UniversalClient (which may
// be a *redis.Client, *redis.ClusterClient, or any other implementation of
// that interface). Optional functional options (WithPrefix, WithTTL) may be
// passed to customize its behavior.
func New(client redis.UniversalClient, opts ...Option) (*Locker, error) {
	if client == nil {
		return nil, fmt.Errorf("redislocker: client must not be nil")
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}

	pool := goredis.NewPool(client)
	rs := redsync.New(pool)

	return &Locker{
		client:  client,
		rs:      rs,
		options: o,
	}, nil
}

// UseIn adds this locker to the passed composer.
func (locker *Locker) UseIn(composer *handler.StoreComposer) {
	composer.UseLocker(locker)
}

// Client returns the underlying redis.UniversalClient, e.g. for health
// checks or graceful shutdown (Close()).
func (locker *Locker) Client() redis.UniversalClient {
	return locker.client
}

// NewLock creates a new unlocked lock object for the given upload ID.
func (locker *Locker) NewLock(id string) (handler.Lock, error) {
	mutex := locker.rs.NewMutex(
		locker.key(id),
		redsync.WithExpiry(locker.options.TTL()),
	)

	return newRedisLock(mutex), nil
}

// key builds the namespaced Redis key used for the distributed lock of the
// given upload ID.
func (locker *Locker) key(id string) string {
	return locker.options.Prefix() + ":" + id
}
