package etcd

import (
    "context"
    "crypto/tls"
    "crypto/x509"
    "io/ioutil"
    "strings"
    "sync"
    "time"

    "go.etcd.io/etcd/client/v3"

    log "github.com/kelseyhightower/confd/log"
)

const (
    DefaultDialTimeout          = 5 * time.Second
    DefaultDialKeepAliveTime    = 10 * time.Second
    DefaultDialKeepAliveTimeout = 3 * time.Second
    DefaultMaxTxnOps            = 128 // Consider making this configurable if needed
    DefaultTxnTimeout           = 3 * time.Second
)

type Watch struct {
    revision int64
    cond     chan struct{}
    mu       sync.RWMutex
}

func NewWatch() *Watch {
    return &Watch{cond: make(chan struct{})}
}

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

func (w *Watch) UpdateRevision(newRevision int64) {
    w.mu.Lock()
    w.revision = newRevision
    close(w.cond)
    w.cond = make(chan struct{})
    w.mu.Unlock()
}

type Client struct {
    client  *clientv3.Client
    watches map[string]*Watch
    mu      sync.Mutex
}

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
                log.Error("watch error: %v", resp.Err())
                continue
            }
            for _, ev := range resp.Events {
                watch.UpdateRevision(ev.Kv.ModRevision)
            }
        }
    }()
    return watch, nil
}
