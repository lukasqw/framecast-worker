package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	DatabaseURL string

	AWSEndpointURL     string
	AWSRegion          string
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	AWSSessionToken    string

	S3BucketRaw    string
	S3BucketOutput string

	SQSQueueURL string
	SQSDLQURL   string // opcional: se definida, ativa o consumer da DLQ (marca ERROR + e-mail)

	// Liga/desliga o envio de e-mail sem mudar qual backend seria usado — permite
	// desativar e reativar sem precisar lembrar/reconfigurar NotifierBackend.
	EmailNotificationsEnabled bool

	// Notifier backend: "smtp" (padrão) ou "ses" (requer SES configurado — não funciona no Academy).
	NotifierBackend string

	// SMTP — usado quando NotifierBackend="smtp". Dev: sem credenciais imprime no stdout.
	SMTPHost     string
	SMTPPort     string
	SMTPUsername string
	SMTPPassword string
	SMTPFrom     string

	// SES — usado quando NotifierBackend="ses". Mantido para ambientes com SES habilitado.
	SESFromEmail         string
	SESRecipientOverride string

	WorkerConcurrency    int
	FFmpegTimeoutMinutes int
	FFmpegFPS            int

	OTelEndpoint string

	AppEnv     string
	AppVersion string
	DDService  string
}

func Load() (*Config, error) {
	cfg := &Config{
		DatabaseURL:          os.Getenv("DATABASE_URL"),
		AWSEndpointURL:       os.Getenv("AWS_ENDPOINT_URL"),
		AWSRegion:            getEnvOrDefault("AWS_REGION", "us-east-1"),
		AWSAccessKeyID:       os.Getenv("AWS_ACCESS_KEY_ID"),
		AWSSecretAccessKey:   os.Getenv("AWS_SECRET_ACCESS_KEY"),
		AWSSessionToken:      os.Getenv("AWS_SESSION_TOKEN"),
		S3BucketRaw:          os.Getenv("S3_BUCKET_RAW"),
		S3BucketOutput:       os.Getenv("S3_BUCKET_OUTPUT"),
		SQSQueueURL:          os.Getenv("SQS_QUEUE_URL"),
		SQSDLQURL:            os.Getenv("SQS_DLQ_URL"),
		NotifierBackend:      getEnvOrDefault("NOTIFIER_BACKEND", "smtp"),
		SMTPHost:             getEnvOrDefault("SMTP_HOST", "sandbox.smtp.mailtrap.io"),
		SMTPPort:             getEnvOrDefault("SMTP_PORT", "2525"),
		SMTPUsername:         os.Getenv("SMTP_USERNAME"),
		SMTPPassword:         os.Getenv("SMTP_PASSWORD"),
		SMTPFrom:             getEnvOrDefault("SMTP_FROM", "noreply@framecast.local"),
		SESFromEmail:         os.Getenv("SES_FROM_EMAIL"),
		SESRecipientOverride: os.Getenv("SES_RECIPIENT_OVERRIDE"),
		OTelEndpoint:         os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		AppEnv:               getEnvOrDefault("APP_ENV", "dev"),
		AppVersion:           getEnvOrDefault("APP_VERSION", "local"),
		DDService:            getEnvOrDefault("DD_SERVICE", "framecast-worker"),
	}

	var err error
	if cfg.EmailNotificationsEnabled, err = parseBool(getEnvOrDefault("EMAIL_NOTIFICATIONS_ENABLED", "true")); err != nil {
		return nil, fmt.Errorf("EMAIL_NOTIFICATIONS_ENABLED inválido: %w", err)
	}
	if cfg.WorkerConcurrency, err = parseInt(getEnvOrDefault("WORKER_CONCURRENCY", "1"), 1); err != nil {
		return nil, fmt.Errorf("WORKER_CONCURRENCY inválido: %w", err)
	}
	if cfg.FFmpegTimeoutMinutes, err = parseInt(getEnvOrDefault("FFMPEG_TIMEOUT_MINUTES", "30"), 1); err != nil {
		return nil, fmt.Errorf("FFMPEG_TIMEOUT_MINUTES inválido: %w", err)
	}
	if cfg.FFmpegFPS, err = parseInt(getEnvOrDefault("FFMPEG_FPS", "1"), 1); err != nil {
		return nil, fmt.Errorf("FFMPEG_FPS inválido: %w", err)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) validate() error {
	var errs []error

	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL é obrigatório"))
	}
	if c.S3BucketRaw == "" {
		errs = append(errs, errors.New("S3_BUCKET_RAW é obrigatório"))
	}
	if c.S3BucketOutput == "" {
		errs = append(errs, errors.New("S3_BUCKET_OUTPUT é obrigatório"))
	}
	if c.SQSQueueURL == "" {
		errs = append(errs, errors.New("SQS_QUEUE_URL é obrigatório"))
	}
	if c.NotifierBackend == "ses" && c.SESFromEmail == "" {
		errs = append(errs, errors.New("SES_FROM_EMAIL é obrigatório quando NOTIFIER_BACKEND=ses"))
	}

	if len(errs) > 0 {
		return fmt.Errorf("configuração inválida: %w", errors.Join(errs...))
	}

	return nil
}

func parseInt(s string, min int) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil || n < min {
		return 0, fmt.Errorf("deve ser um inteiro >= %d, recebido %q", min, s)
	}
	return n, nil
}

func parseBool(s string) (bool, error) {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return false, fmt.Errorf("deve ser true/false, recebido %q", s)
	}
	return b, nil
}

func getEnvOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
