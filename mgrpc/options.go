package mgrpc

import (
	"crypto/tls"
	"time"
)

type Options struct {
	Strategy  Strategy
	Addrs     []string
	Timeout   time.Duration
	Secure    bool
	TLSConfig *tls.Config
	TTL       time.Duration
}

type Option func(*Options)

// Addrs is the registry addresses to use
func Addrs(addrs ...string) Option {
	return func(o *Options) {
		o.Addrs = addrs
	}
}

func Timeout(t time.Duration) Option {
	return func(o *Options) {
		o.Timeout = t
	}
}

// Secure communication with the registry
func Secure(b bool) Option {
	return func(o *Options) {
		o.Secure = b
	}
}

// Specify TLS Config
func TLSConfig(t *tls.Config) Option {
	return func(o *Options) {
		o.TLSConfig = t
	}
}

// Specify RegisterTTL
func TTLConfig(t time.Duration) Option {
	return func(o *Options) {
		o.TTL = t
	}
}

// WithStrategy is the Selector strategy to use
func WithStrategy(s Strategy) Option {
	return func(o *Options) {
		o.Strategy = s
	}
}
