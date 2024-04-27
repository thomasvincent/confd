package backends

import (
	"github.com/kelseyhightower/confd/util"
)

// Config represents the configuration for the backend
type Config struct {
	Auth    *AuthConfig    `toml:"auth"`
	Backend *BackendConfig `toml:"backend"`
}

// AuthConfig represents the authentication configuration
type AuthConfig struct {
	Basic *BasicAuth `toml:"basic_auth"`
	AppID *AppIDAuth `toml:"app_id_auth"`
}

// BackendConfig represents the backend configuration
type BackendConfig struct {
	Etcd  *EtcdConfig  `toml:"etcd"`
	Vault *VaultConfig `toml:"vault"`
}

// BasicAuth represents the basic authentication configuration
type BasicAuth struct {
	Username string `toml:"username"`
	Password string `toml:"password"`
}

// AppIDAuth represents the App ID authentication configuration
type AppIDAuth struct {
	AppID    string `toml:"app_id"`
	RoleID   string `toml:"role_id"`
	SecretID string `toml:"secret_id"`
	UserID   string `toml:"user_id"`
}

// EtcdConfig represents the Etcd backend configuration
type EtcdConfig struct {
	Nodes      util.Nodes `toml:"nodes"`
	ClientCert string     `toml:"client_cert"`
	ClientKey  string     `toml:"client_key"`
	CaKeys     string     `toml:"client_cakeys"`
	Insecure   bool       `toml:"client_insecure"`
}

// VaultConfig represents the Vault backend configuration
type VaultConfig struct {
	AppID string `toml:"app_id"`
	Path   string `toml:"path"`
}
