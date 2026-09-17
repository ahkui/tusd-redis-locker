package redislocker

import "time"

// DefaultPrefix is used to namespace lock keys in Redis when no explicit
// prefix is configured.
const DefaultPrefix = "tusd"

// DefaultTTL is the default lock TTL used by redsync mutexes. This must be
// long enough to comfortably cover the time between two successive PATCH
// requests for the same upload, since tusd re-acquires (or holds) the lock
// for the duration of an upload request.
const DefaultTTL = 30 * time.Second

// options holds the configurable parameters of a Locker.
type options struct {
	prefix string
	ttl    time.Duration
}

// defaultOptions returns the options used when no functional Option is
// provided.
func defaultOptions() options {
	return options{
		prefix: DefaultPrefix,
		ttl:    DefaultTTL,
	}
}

// Option configures a Locker. Options are applied in the order they are
// passed to New / NewFromURL.
type Option func(*options)

// WithPrefix sets the key prefix prepended to every lock key stored in
// Redis, e.g. "<prefix>:<upload-id>". Useful for namespacing when sharing a
// Redis instance/cluster across multiple tusd deployments.
//
// Defaults to DefaultPrefix ("tusd") if not set or set to an empty string.
func WithPrefix(prefix string) Option {
	return func(o *options) {
		o.prefix = prefix
	}
}

// WithTTL sets the TTL applied to each Redis lock. If the process holding
// the lock crashes without releasing it, the lock is automatically released
// after this duration.
//
// Defaults to DefaultTTL (30s) if not set or set to a non-positive value.
func WithTTL(ttl time.Duration) Option {
	return func(o *options) {
		o.ttl = ttl
	}
}

// Prefix returns the configured key prefix, falling back to DefaultPrefix.
func (o *options) Prefix() string {
	if o.prefix == "" {
		return DefaultPrefix
	}
	return o.prefix
}

// TTL returns the configured lock TTL, falling back to DefaultTTL.
func (o *options) TTL() time.Duration {
	if o.ttl <= 0 {
		return DefaultTTL
	}
	return o.ttl
}
