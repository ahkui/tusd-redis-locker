# tusd-redis-locker

A [tusd](https://github.com/tus/tusd) `handler.Locker` implementation backed
by Redis, using [redsync](https://github.com/go-redsync/redsync) for the
distributed mutex algorithm and
[go-redis](https://github.com/redis/go-redis) as the Redis client.

Both **standalone** and **Redis Cluster** deployments are supported, since
the locker is constructed from a `redis.UniversalClient` — pass in a
`*redis.Client` for standalone/sentinel, or a `*redis.ClusterClient` for
Redis Cluster.

## Usage

```go
import (
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ahkui/tusd-redis-locker/pkg/redislocker"
	"github.com/tus/tusd/v2/pkg/handler"
)

client := redis.NewClient(&redis.Options{ /* ... */ })
// or: client := redis.NewClusterClient(&redis.ClusterOptions{ /* ... */ })

// Defaults: prefix "tusd", TTL 30s.
locker, err := redislocker.New(client)

// Custom prefix.
locker, err := redislocker.New(client, redislocker.WithPrefix("my-app"))

// Custom prefix and TTL.
locker, err := redislocker.New(client,
	redislocker.WithPrefix("my-app"),
	redislocker.WithTTL(15*time.Second),
)

composer := handler.NewStoreComposer()
locker.UseIn(composer)
```

To build a `redis.Options` / `redis.ClusterOptions` from a connection URL of
the form `redis://[username]:[password]@[host]:[port]/[database_number]`,
use go-redis's own `redis.ParseURL` / `redis.ParseClusterURL` helpers before
calling `redis.NewClient` / `redis.NewClusterClient`. Note that Redis Cluster
does not support the `SELECT` command, so any database number in the URL is
ignored in that case.

## How locking works

Each upload ID maps to a Redis key `<prefix>:<id>`. Acquiring a lock creates
this key with a TTL (default 30s, override with `WithTTL`) using redsync's
distributed mutex algorithm, guaranteeing mutual exclusion even across
multiple tusd processes/replicas sharing the same Redis backend. If a
process holding a lock crashes without releasing it, the key automatically
expires and the lock becomes available again.

## Testing

The test suite runs entirely in-process using
[miniredis](https://github.com/alicebob/miniredis) as an in-memory Redis
stand-in, so no external Redis server is required:

```bash
go test ./...
```
