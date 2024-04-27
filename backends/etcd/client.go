// Package etcd provides a client for managing communication with an etcd server.
// This includes support for watching changes and retrieving values based
// on key prefixes.
package etcd

import (
    "context"
    "crypto/tls"
    "crypto/x509"
    "fmt"
    "sync"
    "time"

    "go.etcd.io/etcd/client/v3"
)

// Constants for default configuration settings.
const (
    DefaultDialTimeout          = 5 * time.Second
    DefaultDialKeepAliveTime    = 10 * time.Second
    DefaultDialKeepAliveTimeout = 3 * time.Second
    DefaultMaxTxnOps            = 128 // Update to make this configurable if required.
    DefaultTxnTimeout           = 3 * time.Second
)

// Watch provides a mechanism to monitor changes in etcd for a specific key prefix.
// It maintains state regarding the last seen revision and provides functionality
// to wait for newer revisions.
type Watch struct {
    revision int64
    cond     chan struct{}
    mu       sync.RWMutex
}

// NewWatch creates a new instance of Watch.
func NewWatch() *Watch {
    return &Watch{cond: make(chan struct{})}
}

// WaitRevision blocks until a revision greater than lastRevision is observed.
// Notifies via a channel once an updated revision is available or context expires.
func (w *Watch) WaitRevision(ctx context.Context, lastRevision int64, notify chan<- int64) {
    for {
        w.mu.RLock()
        if w.revision > lastRevision {
            w.mu.RUnlock()
            select {
            case notify <- w.revision:
            case <-ctx.Done():
            }
            return
        }
        cond := w.cond
        w.mu.RUnlock()

        select {
        case <-cond:
        case <-ctx.Done():
            return
        }
    }
}

// UpdateRevision updates the current revision to newRevision and notifies
// all waiting goroutines.
func (w *Watch) UpdateRevision(newRevision int64) {
    w.mu.Lock()
    w.revision = newRevision
    close(w.cond)
    w.cond = make(chan struct{})
    w.mu.Unlock()
}

// Client manages connections to the etcd server and provides methods for
// retrieving values and watching keys.
type Client struct {
    client  *clientv3.Client
    watches map[string]*Watch
    mu      sync.Mutex
}

// NewEtcdClient initializes and returns a new etcd client using the specified configuration.
func NewEtcdClient(cfg clientv3.Config) (*Client, error) {
    client, err := clientv3.New(cfg)
    if err != nil {
        return nil, err
    }
    return &Client{
        client:  client,
        watches: make(map[string]*Watch),
    }, nil
}

// SetupTLSConfig sets up and validates TLS configurations using given certificates.
func SetupTLSConfig(cert, key, caCert string) (*tls.Config, error) {
    var (
        tlsConfig = &tls.Config{}
        certs     []tls.Certificate
        pool      *x509.CertPool
        err       error
    )

    if cert != "" && key != "" {
        certs, err = tls.X509KeyPair([]byte(cert), []byte(key))
        if err != nil {
            return nil, err
        }
        tlsConfig.Certificates = append(tlsConfig.Certificates, certs)
    }

    if caCert != "" {
        pool = x509.NewCertPool()
        if !pool.AppendCertsFromPEM([]byte(caCert)) {
            return nil, fmt.Errorf("failed to append CA certificate")
        }
        tlsConfig.RootCAs = pool
    }

    return tlsConfig, nil
}

// GetValues retrieves values for the specified keys, applying a prefix search.
// Returns a map of the results or any error encountered during the retrieval.
func (c *Client) GetValues(ctx context.Context, keys []string) (map[string]string, error) {
    values := make(map[string]string)
    for _, key := range keys {
        resp, err := c.client.Get(ctx, key, clientv3.WithPrefix())
        if err != nil {
            return nil, err
        }
        for _, kv := range resp.Kvs {
            values[string(kv.Key)] = string(kv.Value)
        }
    }
    return values, nil
}

// setupWatch initializes and adds a watch for a given key unless it already exists.
// Returns the existing or new watch and any error that occurs during setup.
func (c *Client) setupWatch(ctx context.Context, key string) (*Watch, error) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if watch, exists := c.watches[key]; exists {
        return watch, nil
    }
    watch := NewWatch()
    c.watches[key] = watch

    go func() {
        var wch clientv3.WatchChan = c.client.Watch(ctx, key, clientv3.WithPrefix(), clientv3.WithRev(watch.revision))
        for resp := range wch {
            if resp.Err() != nil {
                fmt.Printf("watch error: %v\n", resp.Err())
                continue
            }
            for _, ev := range resp.Events {
                watch.UpdateRevision(ev.Kv.ModRevision)
            }
        }
    }()
    return watch, nil
}
