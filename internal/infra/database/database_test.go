package database

import (
	"testing"

	appconfig "github.com/lukasqw/framecast-worker/internal/config"
	"github.com/stretchr/testify/require"
)

// TestConnect_DSNInvalido_RetornaErro exercita o caminho de erro de Connect sem
// precisar de um Postgres real: uma porta fechada falha rápido na conexão TCP.
func TestConnect_DSNInvalido_RetornaErro(t *testing.T) {
	cfg := &appconfig.Config{
		DatabaseURL: "postgres://user:pass@127.0.0.1:1/db?sslmode=disable&connect_timeout=1",
		AppEnv:      "dev",
	}

	_, err := Connect(cfg)
	require.Error(t, err)
}

func TestConnect_AppEnvProd_DSNInvalido_RetornaErro(t *testing.T) {
	cfg := &appconfig.Config{
		DatabaseURL: "postgres://user:pass@127.0.0.1:1/db?sslmode=disable&connect_timeout=1",
		AppEnv:      "prod",
	}

	_, err := Connect(cfg)
	require.Error(t, err)
}
