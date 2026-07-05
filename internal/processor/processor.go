package processor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/lukasqw/framecast-worker/internal/consumer"
	"github.com/lukasqw/framecast-worker/internal/infra/email"
	"github.com/lukasqw/framecast-worker/internal/infra/observability"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"gorm.io/gorm"
)

// downloadURLTTL casa com o lifecycle de expiração do bucket de output (7 dias,
// ver framecast-infra) — o link do e-mail fica válido por tanto tempo quanto o
// arquivo ainda existir no S3.
const downloadURLTTL = 7 * 24 * time.Hour

// Processor implementa consumer.Handler.
type Processor struct {
	db                   *gorm.DB
	s3Client             s3API
	uploader             uploader
	presigner            presignAPI
	sqsClient            sqsAPI
	queueURL             string
	workerID             string
	ffmpegTimeoutMinutes int
	ffmpegFPS            int
	notifier             email.Notifier
}

func New(
	db *gorm.DB,
	s3Client s3API,
	presigner presignAPI,
	sqsClient sqsAPI,
	notif email.Notifier,
	queueURL string,
	ffmpegTimeoutMinutes int,
	ffmpegFPS int,
) *Processor {
	hostname, _ := os.Hostname()
	return &Processor{
		db:                   db,
		s3Client:             s3Client,
		uploader:             manager.NewUploader(s3Client), //nolint:staticcheck // transfermanager (substituto) ainda é experimental no aws-sdk-go-v2
		presigner:            presigner,
		sqsClient:            sqsClient,
		queueURL:             queueURL,
		workerID:             hostname,
		ffmpegTimeoutMinutes: ffmpegTimeoutMinutes,
		ffmpegFPS:            ffmpegFPS,
		notifier:             notif,
	}
}

func (p *Processor) Process(ctx context.Context, msg *consumer.Message, receiptHandle string) error {
	// Span raiz do processamento — filho do span consumer (trace distribuído).
	ctx, rootSpan := observability.SpanWorker(ctx, "worker.process")
	defer rootSpan.End()
	rootSpan.SetAttributes(
		attribute.String("video_id", msg.VideoID),
		attribute.String("user_id", msg.UserID),
	)

	log := observability.LoggerFromContext(ctx).With(slog.String("video_id", msg.VideoID))
	start := time.Now()

	// ── Idempotência + lease ──────────────────────────────────────────────────
	row, err := acquireLease(ctx, p.db, msg.VideoID, p.workerID)
	if errors.Is(err, errAlreadyDone) || errors.Is(err, errAlreadyError) {
		log.InfoContext(ctx, "vídeo já finalizado — descartando mensagem SQS")
		return p.deleteMessage(ctx, receiptHandle)
	}
	if errors.Is(err, errLeaseHeld) {
		// Worker vivo já processa este vídeo (heartbeat fresco). Suprime a duplicata
		// deletando a mensagem — o worker original conclui e grava DONE.
		log.InfoContext(ctx, "vídeo em processamento por worker vivo — suprimindo duplicata")
		return p.deleteMessage(ctx, receiptHandle)
	}
	if err != nil {
		rootSpan.RecordError(err)
		rootSpan.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("falha ao adquirir lease: %w", err)
	}

	log.InfoContext(ctx, "lease adquirido", slog.Int("attempt", row.Attempt))

	stopHeartbeat, heartbeatErrCh := startHeartbeat(ctx, p.sqsClient, p.queueURL, receiptHandle, p.db, msg.VideoID, heartbeatInterval)
	defer stopHeartbeat()

	// ── Diretório temporário — limpo sempre ao final ──────────────────────────
	tempDir := filepath.Join(os.TempDir(), msg.VideoID)
	if err := os.MkdirAll(tempDir, 0o700); err != nil {
		return fmt.Errorf("falha ao criar diretório temporário: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// ── Download do vídeo original do S3 ─────────────────────────────────────
	inputPath := filepath.Join(tempDir, "input.mp4")
	ctx, dlSpan := observability.SpanWorker(ctx, "s3.download")
	dlSpan.SetAttributes(
		attribute.String("aws.s3.bucket", msg.Bucket),
		attribute.String("aws.s3.key", msg.S3Key),
	)
	dlErr := downloadVideo(ctx, p.s3Client, msg.Bucket, msg.S3Key, inputPath)
	if dlErr != nil {
		dlSpan.RecordError(dlErr)
		dlSpan.SetStatus(codes.Error, dlErr.Error())
	}
	dlSpan.End()
	if dlErr != nil {
		return fmt.Errorf("falha ao baixar vídeo: %w", dlErr) // retentável
	}
	if err := checkHeartbeat(ctx, msg.VideoID, heartbeatErrCh); err != nil {
		return err
	}

	// ── FFmpeg ────────────────────────────────────────────────────────────────
	framesDir := filepath.Join(tempDir, "frames")
	if err := os.MkdirAll(framesDir, 0o700); err != nil {
		return fmt.Errorf("falha ao criar diretório de frames: %w", err)
	}

	ctx, ffSpan := observability.SpanWorker(ctx, "ffmpeg.execute")
	ffmpegStart := time.Now()
	frameCount, err := runFFmpeg(ctx, inputPath, framesDir, p.ffmpegTimeoutMinutes, p.ffmpegFPS)
	observability.RecordFFmpegDuration(ctx, time.Since(ffmpegStart).Seconds())

	if err != nil {
		// Não-retentável: vídeo corrompido, codec inválido ou timeout
		ffSpan.RecordError(err)
		ffSpan.SetStatus(codes.Error, err.Error())
		ffSpan.End()
		log.ErrorContext(ctx, "FFmpeg falhou", slog.String("erro", err.Error()))
		userEmail, _ := p.getUserEmail(ctx, row.UserID)
		p.markError(ctx, msg.VideoID, err.Error())
		if userEmail != "" {
			p.notifier.SendFailure(ctx, userEmail, msg.VideoID, err.Error())
		}
		observability.RecordVideoProcessed(ctx, "error")
		return p.deleteMessage(ctx, receiptHandle)
	}

	ffSpan.SetAttributes(attribute.Int("ffmpeg.frame_count", frameCount))
	ffSpan.End()
	observability.RecordFrameCount(ctx, int64(frameCount))
	if err := checkHeartbeat(ctx, msg.VideoID, heartbeatErrCh); err != nil {
		return err
	}

	// ── ZIP streaming + upload S3 ─────────────────────────────────────────────
	ctx, zipSpan := observability.SpanWorker(ctx, "zip.upload")
	zipSpan.SetAttributes(attribute.String("aws.s3.bucket", msg.OutputBucket))
	outputKey, err := zipAndUpload(ctx, p.uploader, framesDir, msg.OutputBucket, msg.VideoID)
	if err != nil {
		zipSpan.RecordError(err)
		zipSpan.SetStatus(codes.Error, err.Error())
		zipSpan.End()
		return fmt.Errorf("falha no zip+upload: %w", err) // retentável
	}
	zipSpan.SetAttributes(attribute.String("aws.s3.key", outputKey))
	zipSpan.End()
	if err := checkHeartbeat(ctx, msg.VideoID, heartbeatErrCh); err != nil {
		return err
	}

	// ── Finalização em transação ──────────────────────────────────────────────
	if err := p.markDone(ctx, msg.VideoID, outputKey, frameCount); err != nil {
		return fmt.Errorf("falha ao finalizar vídeo no banco: %w", err) // retentável
	}

	// ── Notificação de sucesso (best-effort) ──────────────────────────────────
	if userEmail, err := p.getUserEmail(ctx, row.UserID); err == nil && userEmail != "" {
		downloadURL := p.presignOutputURL(ctx, msg.OutputBucket, outputKey)
		p.notifier.SendSuccess(ctx, userEmail, msg.VideoID, row.OriginalName, downloadURL)
	}

	observability.RecordVideoProcessed(ctx, "done")
	observability.RecordVideoProcessingDuration(ctx, time.Since(start).Seconds(), "done")

	// ── ACK ───────────────────────────────────────────────────────────────────
	return p.deleteMessage(ctx, receiptHandle)
}

func (p *Processor) markDone(ctx context.Context, videoID, outputKey string, frameCount int) error {
	res := p.db.WithContext(ctx).Exec(
		`UPDATE videos SET status = 'DONE', s3_key_output = ?, frame_count = ?,
		  worker_id = NULL, updated_at = NOW() WHERE id = ?`,
		outputKey, frameCount, videoID,
	)
	return res.Error
}

// presignOutputURL gera a URL de download do ZIP para o e-mail de sucesso.
// Best-effort: falha aqui não deve impedir a notificação (o app segue oferecendo
// o download via GetVideo), só sai sem o link direto.
func (p *Processor) presignOutputURL(ctx context.Context, bucket, key string) string {
	req, err := p.presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(downloadURLTTL))
	if err != nil {
		observability.LoggerFromContext(ctx).WarnContext(ctx, "falha ao gerar url de download para o e-mail (best-effort)",
			slog.String("erro", err.Error()))
		return ""
	}
	return req.URL
}

// markError atualiza o status para ERROR. Best-effort — falha apenas logada.
func (p *Processor) markError(ctx context.Context, videoID, errMsg string) {
	if len(errMsg) > 500 {
		errMsg = errMsg[:500]
	}
	if res := p.db.WithContext(ctx).Exec(
		`UPDATE videos SET status = 'ERROR', error_message = ?, updated_at = NOW() WHERE id = ?`,
		errMsg, videoID,
	); res.Error != nil {
		slog.Error("falha ao marcar vídeo como ERROR",
			slog.String("video_id", videoID),
			slog.String("erro", res.Error.Error()),
		)
	}
}

func (p *Processor) getUserEmail(ctx context.Context, userID string) (string, error) {
	var email string
	err := p.db.WithContext(ctx).Raw("SELECT email FROM users WHERE id = ?", userID).Scan(&email).Error
	return email, err
}

func (p *Processor) deleteMessage(ctx context.Context, receiptHandle string) error {
	_, err := p.sqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(p.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	})
	if err != nil {
		return fmt.Errorf("falha ao deletar mensagem SQS: %w", err)
	}
	return nil
}

// checkHeartbeat verifica de forma não-bloqueante se o heartbeat reportou falha.
// Retorna erro (sem DeleteMessage) para que o SQS reentregue a mensagem após a
// visibility expirar — o lease TTL garante que outro pod poderá reassumir.
func checkHeartbeat(ctx context.Context, videoID string, ch <-chan error) error {
	select {
	case err := <-ch:
		slog.ErrorContext(ctx, "heartbeat falhou — abandonando processamento para SQS reentregar",
			slog.String("video_id", videoID),
			slog.String("erro", err.Error()),
		)
		return fmt.Errorf("heartbeat falhou: %w", err)
	default:
		return nil
	}
}
