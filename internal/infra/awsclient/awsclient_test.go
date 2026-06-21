package awsclient

import (
	"context"
	"testing"

	appconfig "github.com/lukasqw/framecast-worker/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_ComCredenciaisEstaticasEEndpointCustomizado(t *testing.T) {
	cfg := &appconfig.Config{
		AWSRegion:          "us-east-1",
		AWSAccessKeyID:     "test",
		AWSSecretAccessKey: "test",
		AWSEndpointURL:     "http://localhost:4566",
	}

	clients, err := New(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, clients)
	assert.NotNil(t, clients.S3)
	assert.NotNil(t, clients.SQS)
	assert.NotNil(t, clients.SES)
}

func TestNew_SemCredenciaisEstaticasNemEndpoint(t *testing.T) {
	cfg := &appconfig.Config{
		AWSRegion: "us-east-1",
	}

	clients, err := New(context.Background(), cfg)
	require.NoError(t, err)
	require.NotNil(t, clients)
	assert.NotNil(t, clients.S3)
	assert.NotNil(t, clients.SQS)
	assert.NotNil(t, clients.SES)
}
