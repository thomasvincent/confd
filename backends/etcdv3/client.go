// Package etcdv3 provides a client for managing communication with an etcd server.
// It supports operations like watching changes and retrieving values based on key prefixes.
package etcdv3

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"github.com/kelseyhightower/confd/log"
	"sync"
)

// Watch struct encapsulates the details for watching changes in etcd keys.
type Watch struct {
	revision int64        // Last seen revision
	cond     chan struct{} // Channel that signals revision changes
	rwl      sync.RWMutex  // Mutex to protect access to the cond channel
}

// NewWatch initializes a new Watch object.
func NewWatch() *Watch {
	return &Watch{cond: make(chan struct{})}
}

// WaitNext blocks until the revision is updated or context is cancelled.
func (w *Watch) WaitNext(ctx context.Context, lastRevision int64, notify chan<- int64) {
	for {
		w.rwl.RLock()
		cond := w.cond
		rev := w.revision
		w.rwl.RUnlock()

		if rev > lastRevision {
			select {
			case notify <- rev:
			case <-ctx.Done():
				return
			}
			return
		}

		select {
		case <-cond:
		case <-ctx.Done():
			return
		}
	}
}

// update safely updates the watch's revision and signals any waiting goroutines.
func (w *Watch) update(newRevision int64) {
	w.rwl.Lock()
	defer w.rwl.Unlock()
	w.revision = newRevision
	close(w.cond)
	w.cond = make(chan struct{})
}

// createWatch sets up a watch on the specified prefix.
func createWatch(client *clientv3.Client, prefix string) (*Watch, error) {
	w := NewWatch()
	go func() {
		for {
			rch := client.Watch(context.Background(), prefix, clientv3.WithPrefix(), clientv3.WithRev(w.revision+1))
			for wresp := range rch {
				if wresp.Err() != nil {
					log.Error("watch error: %v", wresp.Err())
					continue
				}

				for _, ev := range wresp.Events {
					w.update(ev.Kv.ModRevision)
				}
			}

			time.Sleep(1 * time.Second) // Throttle reconnection attempts
		}
	}()

	return w, nil
}

// Client wraps the etcdv3 client library to manage connections and watches.
type Client struct {
	client  *clientv3.Client
	watches map[string]*Watch
	wm      sync.Mutex
}

// NewEtcdClient creates a new Client instance with provided configuration.
func NewEtcdClient(machines []string, cert, key, caCert string, basicAuth bool, username, password string) (*Client, error) {
	tlsConfig, err := setupTLS(cert, key, caCert)
	if err != nil {
		return nil, err
	}

	cfg := clientv3.Config{
		Endpoints:   machines,
		DialTimeout: 5 * time.Second,
		TLS:         tlsConfig,
	}

	if basicAuth {
		cfg.Username = username
		cfg.Password = password
	}

	client, err := clientv3.New(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}

	return &Client{client: client, watches: make(map[string]*Watch)}, nil
}

// setupTLS configures TLS based on provided certificate details.
func setupTLS(cert, key, caCert string) (*tls.Config, error) {
	tlsConfig := &tls.Config{}

	if len(caCert) > 0 {
		caCertPool := x509.NewCertPool()
		caCertBytes, err := ioutil.ReadFile(caCert)
		if err != nil {
			return nil, err
		}
		if !caCertPool.AppendCertsFromPEM(caCertBytes) {
			return nil, fmt.Errorf("failed to append CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	if len(cert) > 0 && len(key) > 0 {
		certificate, err := tls.LoadX509KeyPair(cert, key)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	return tlsConfig, nil
}
