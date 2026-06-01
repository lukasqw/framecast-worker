package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	appconfig "github.com/lukasqw/framecast-worker/internal/config"
	"github.com/lukasqw/framecast-worker/internal/consumer"
	"github.com/lukasqw/framecast-worker/internal/infra/awsclient"
	"github.com/lukasqw/framecast-worker/internal/infra/database"
	"github.com/lukasqw/framecast-worker/internal/infra/observability"
	"github.com/lukasqw/framecast-worker/internal/processor"
)

func main() {
	_ = godotenv.Load()

	observability.NewLogger()

	cfg, err := appconfig.Load()
	if err != nil {
		slog.Error("configuração inválida", slog.String("erro", err.Error()))
		os.Exit(1)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	otelShutdown, err := observability.InitOTel(ctx)
	if err != nil {
		slog.Error("falha ao inicializar OTel", slog.String("erro", err.Error()))
		os.Exit(1)
	}
	defer func() {
		if err := otelShutdown(context.Background()); err != nil {
			slog.Error("falha no shutdown do OTel", slog.String("erro", err.Error()))
		}
	}()

	db, err := database.Connect(cfg)
	if err != nil {
		slog.Error("falha ao conectar ao banco", slog.String("erro", err.Error()))
		os.Exit(1)
	}
	sqlDB, _ := db.DB()
	defer sqlDB.Close()

	aws, err := awsclient.New(ctx, cfg)
	if err != nil {
		slog.Error("falha ao inicializar clientes AWS", slog.String("erro", err.Error()))
		os.Exit(1)
	}

	slog.Info("worker iniciado",
		slog.String("env", cfg.AppEnv),
		slog.Int("concorrência", cfg.WorkerConcurrency),
	)

	proc := processor.New(db, aws.S3, aws.SQS, aws.SES, cfg.SQSQueueURL, cfg.SESFromEmail, cfg.SESRecipientOverride, cfg.FFmpegTimeoutMinutes)
	c := consumer.New(aws.SQS, cfg.SQSQueueURL, cfg.WorkerConcurrency, proc)

	// Run bloqueia até ctx ser cancelado (SIGINT/SIGTERM) e aguarda goroutines ativas
	c.Run(ctx)

	slog.Info("worker encerrado")
}
