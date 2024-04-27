package backends

import (
	"errors"
	"strings"

	"github.com/kelseyhightower/confd/backends/consul"
	"github.com/kelseyhightower/confd/backends/dynamodb"
	"github.com/kelseyhightower/confd/backends/env"
	"github.com/kelseyhightower/confd/backends/etcdv3"
	"github.com/kelseyhightower/confd/backends/file"
	"github.com/kelseyhightower/confd/backends/rancher"
	"github.com/kelseyhightower/confd/backends/redis"
	"github.com/kelseyhightower/confd/backends/ssm"
	"github.com/kelseyhightower/confd/backends/vault"
	"github.com/kelseyhightower/confd/backends/zookeeper"
	"github.com/kelseyhightower/confd/log"
)

// The StoreClient interface is implemented by objects that can retrieve
// key/value pairs from a backend store.
type StoreClient interface {
	GetValues(keys []string) (map[string]string, error)
	WatchPrefix(prefix string, keys []string, waitIndex uint64, stopChan chan bool) (uint64, error)
}

const (
	defaultBackend = "etcd"
)

// New is used to create a storage client based on our configuration.
func New(config Config) (StoreClient, error) {
	if config.Backend == "" {
		config.Backend = defaultBackend
	}

	log.Infof("Backend set to '%s'", config.Backend)

	var client StoreClient
	var err error

	switch config.Backend {
	case "consul":
		client, err = consul.New(config.BackendNodes, config.Scheme, config.ClientCert, config.ClientKey, config.ClientCaKeys, config.BasicAuth, config.Username, config.Password)
	case "etcd", "etcdv3":
		client, err = etcdv3.NewEtcdClient(config.BackendNodes, config.ClientCert, config.ClientKey, config.ClientCaKeys, config.BasicAuth, config.Username, config.Password)
	case "zookeeper":
		client, err = zookeeper.NewZookeeperClient(config.BackendNodes)
	case "rancher":
		client, err = rancher.NewRancherClient(config.BackendNodes)
	case "redis":
		client, err = redis.NewRedisClient(config.BackendNodes, config.ClientKey, config.Separator)
	case "env":
		client, err = env.NewEnvClient()
	case "file":
		client, err = file.NewFileClient(config.YAMLFile, config.Filter)
	case "vault":
		vaultConfig := map[string]string{
			"app-id":    config.AppID,
			"user-id":   config.UserID,
			"role-id":   config.RoleID,
			"secret-id": config.SecretID,
			"username":  config.Username,
			"password":  config.Password,
			"token":     config.AuthToken,
			"cert":      config.ClientCert,
			"key":       config\ClientKey,
			"caCert":    config.ClientCaKeys,
			"path":      config.Path,
		}
		client, err = vault.New(config.BackendNodes[0], config.AuthType, vaultConfig)
	case "dynamodb":
		log.Infof("DynamoDB table set to '%s'", config.Table)
		client, err = dynamodb.NewDynamoDBClient(config.Table)
	case "ssm":
		client, err = ssm.New()
	default:
		return nil, errors.New("Invalid backend")
	}

	if err != nil {
		return nil, err
	}

	return client, nil
}
