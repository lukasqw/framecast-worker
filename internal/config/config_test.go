package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setRequiredEnv define todas as variáveis obrigatórias com valores válidos.
// Usa t.Setenv, que restaura o valor original automaticamente ao fim do teste.
func setRequiredEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")
	t.Setenv("S3_BUCKET_RAW", "framecast-raw")
	t.Setenv("S3_BUCKET_OUTPUT", "framecast-output")
	t.Setenv("SQS_QUEUE_URL", "http://localhost:4566/000000000000/queue")
	t.Setenv("SES_FROM_EMAIL", "noreply@framecast.local")
	t.Setenv("AWS_ENDPOINT_URL", "")
	t.Setenv("AWS_REGION", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")
	t.Setenv("SQS_DLQ_URL", "")
	t.Setenv("SES_RECIPIENT_OVERRIDE", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("APP_ENV", "")
	t.Setenv("APP_VERSION", "")
	t.Setenv("DD_SERVICE", "")
	t.Setenv("WORKER_CONCURRENCY", "")
	t.Setenv("FFMPEG_TIMEOUT_MINUTES", "")
	t.Setenv("FFMPEG_FPS", "")
}

func TestLoad_Sucesso(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "postgres://user:pass@localhost:5432/db", cfg.DatabaseURL)
	assert.Equal(t, "framecast-raw", cfg.S3BucketRaw)
	assert.Equal(t, "framecast-output", cfg.S3BucketOutput)
	assert.Equal(t, "http://localhost:4566/000000000000/queue", cfg.SQSQueueURL)
	assert.Equal(t, "noreply@framecast.local", cfg.SESFromEmail)
}

func TestLoad_DefaultsAplicados(t *testing.T) {
	setRequiredEnv(t)

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "us-east-1", cfg.AWSRegion)
	assert.Equal(t, "dev", cfg.AppEnv)
	assert.Equal(t, "local", cfg.AppVersion)
	assert.Equal(t, "framecast-worker", cfg.DDService)
	assert.Equal(t, 1, cfg.WorkerConcurrency)
	assert.Equal(t, 30, cfg.FFmpegTimeoutMinutes)
	assert.Equal(t, 1, cfg.FFmpegFPS)
}

func TestLoad_ValoresCustomizados(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("AWS_REGION", "sa-east-1")
	t.Setenv("APP_ENV", "prod")
	t.Setenv("WORKER_CONCURRENCY", "5")
	t.Setenv("FFMPEG_TIMEOUT_MINUTES", "10")
	t.Setenv("FFMPEG_FPS", "2")

	cfg, err := Load()
	require.NoError(t, err)
	assert.Equal(t, "sa-east-1", cfg.AWSRegion)
	assert.Equal(t, "prod", cfg.AppEnv)
	assert.Equal(t, 5, cfg.WorkerConcurrency)
	assert.Equal(t, 10, cfg.FFmpegTimeoutMinutes)
	assert.Equal(t, 2, cfg.FFmpegFPS)
}

func TestLoad_FaltandoObrigatorias(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("DATABASE_URL", "")
	t.Setenv("S3_BUCKET_RAW", "")
	t.Setenv("S3_BUCKET_OUTPUT", "")
	t.Setenv("SQS_QUEUE_URL", "")
	t.Setenv("SES_FROM_EMAIL", "")

	_, err := Load()
	require.Error(t, err)
	for _, want := range []string{
		"DATABASE_URL", "S3_BUCKET_RAW", "S3_BUCKET_OUTPUT", "SQS_QUEUE_URL",
	} {
		assert.Contains(t, err.Error(), want)
	}
}

func TestLoad_WorkerConcurrencyInvalido(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("WORKER_CONCURRENCY", "abc")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WORKER_CONCURRENCY")
}

func TestLoad_WorkerConcurrencyMenorQueMinimo(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("WORKER_CONCURRENCY", "0")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WORKER_CONCURRENCY")
}

func TestLoad_FFmpegTimeoutInvalido(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("FFMPEG_TIMEOUT_MINUTES", "-1")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FFMPEG_TIMEOUT_MINUTES")
}

func TestLoad_FFmpegFPSInvalido(t *testing.T) {
	setRequiredEnv(t)
	t.Setenv("FFMPEG_FPS", "0")

	_, err := Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "FFMPEG_FPS")
}
