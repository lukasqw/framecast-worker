package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/joho/godotenv"
	appconfig "github.com/lukasqw/framecast-worker/internal/config"
	"github.com/lukasqw/framecast-worker/internal/consumer"
	"github.com/lukasqw/framecast-worker/internal/infra/awsclient"
	"github.com/lukasqw/framecast-worker/internal/infra/database"
	"github.com/lukasqw/framecast-worker/internal/infra/email"
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

	// Seleciona backend de notificação via NOTIFIER_BACKEND (smtp | ses)
	var notif email.Notifier
	switch cfg.NotifierBackend {
	case "ses":
		notif = email.NewSESNotifier(aws.SES, cfg.SESFromEmail, cfg.SESRecipientOverride)
		slog.Info("notifier: SES", slog.String("from", cfg.SESFromEmail))
	default: // smtp
		notif = email.NewSMTPNotifier(cfg.SMTPHost, cfg.SMTPPort, cfg.SMTPUsername, cfg.SMTPPassword, cfg.SMTPFrom)
		slog.Info("notifier: SMTP", slog.String("host", cfg.SMTPHost), slog.String("from", cfg.SMTPFrom))
	}

	slog.Info("worker iniciado",
		slog.String("env", cfg.AppEnv),
		slog.Int("concorrência", cfg.WorkerConcurrency),
	)

	proc := processor.New(db, aws.S3, aws.SQS, notif, cfg.SQSQueueURL, cfg.FFmpegTimeoutMinutes, cfg.FFmpegFPS)
	c := consumer.New(aws.SQS, cfg.SQSQueueURL, cfg.WorkerConcurrency, proc)

	// Consumer da DLQ (opcional): marca o vídeo como ERROR e notifica o usuário.
	var wg sync.WaitGroup
	if cfg.SQSDLQURL != "" {
		dlqHandler := processor.NewDLQHandler(db, aws.SQS, notif, cfg.SQSDLQURL)
		dlqConsumer := consumer.New(aws.SQS, cfg.SQSDLQURL, 1, dlqHandler)
		wg.Go(func() {
			dlqConsumer.Run(ctx)
		})
		slog.Info("consumer DLQ iniciado", slog.String("fila", cfg.SQSDLQURL))
	}

	// Run bloqueia até ctx ser cancelado (SIGINT/SIGTERM) e aguarda goroutines ativas
	c.Run(ctx)
	wg.Wait()

	slog.Info("worker encerrado")
}
