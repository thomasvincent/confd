package consul

import (
	"context"
	"path"
	"strings"

	"github.com/hashicorp/consul/api"
)

// ConsulClient is a wrapper around the Consul KV client
type ConsulClient struct {
	client *api.KV
}

// NewConsulClient creates a new Consul client
func NewConsulClient(ctx context.Context, nodes []string, scheme, cert, key, caCert string, basicAuth bool, username string, password string) (*ConsulClient, error) {
	conf := api.DefaultConfig()
	conf.Scheme = scheme

	if len(nodes) > 0 {
		conf.Address = nodes[0]
	}

	if basicAuth {
		conf.HttpAuth = &api.HttpBasicAuth{
			Username: username,
			Password: password,
		}
	}

	if cert != "" && key != "" {
		conf.TLSConfig.CertFile = cert
		conf.TLSConfig.KeyFile = key
	}
	if caCert != "" {
		conf.TLSConfig.CAFile = caCert
	}

	client, err := api.NewClient(conf)
	if err != nil {
		return nil, err
	}
	return &ConsulClient{client.KV()}, nil
}

// GetValue queries Consul for a single key
func (c *ConsulClient) GetValue(ctx context.Context, key string) (string, error) {
	key = strings TrimPrefix(key, "/")
	pair, _, err := c.client.Get(ctx, key, nil)
	if err != nil {
		return "", err
	}
	return string(pair.Value), nil
}

// GetValues queries Consul for multiple keys
func (c *ConsulClient) GetValues(ctx context.Context, keys []string) (map[string]string, error) {
	values := make(map[string]string)
	for _, key := range keys {
		value, err := c.GetValue(ctx, key)
		if err != nil {
			return nil, err
		}
		values[key] = value
	}
	return values, nil
}

// WatchPrefix watches a Consul prefix for changes
func (c *ConsulClient) WatchPrefix(ctx context.Context, prefix string, waitIndex uint64, stopChan chan bool) (uint64, error) {
	respChan := make(chan watchResponse)
	go func() {
	 opts := api.QueryOptions{
			WaitIndex: waitIndex,
		}
		_, meta, err := c.client.List(ctx, prefix, &opts)
		if err != nil {
			respChan <- watchResponse{waitIndex, err}
			return
		}
		respChan <- watchResponse{meta.LastIndex, err}
	}()

	select {
	case <-stopChan:
		return waitIndex, nil
	case r := <-respChan:
		return r.waitIndex, r.err
	}
}

type watchResponse struct {
	waitIndex uint64
	err       error
}
