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
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/lukasqw/framecast-worker/internal/consumer"
	"github.com/lukasqw/framecast-worker/internal/infra/email"
	"github.com/lukasqw/framecast-worker/internal/infra/observability"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"gorm.io/gorm"
)

// Processor implementa consumer.Handler.
type Processor struct {
	db                   *gorm.DB
	s3Client             s3API
	uploader             uploader
	sqsClient            sqsAPI
	queueURL             string
	workerID             string
	ffmpegTimeoutMinutes int
	notifier             email.Notifier
}

func New(
	db *gorm.DB,
	s3Client s3API,
	sqsClient sqsAPI,
	notif email.Notifier,
	queueURL string,
	ffmpegTimeoutMinutes int,
) *Processor {
	hostname, _ := os.Hostname()
	return &Processor{
		db:                   db,
		s3Client:             s3Client,
		uploader:             manager.NewUploader(s3Client), //nolint:staticcheck // transfermanager (substituto) ainda é experimental no aws-sdk-go-v2
		sqsClient:            sqsClient,
		queueURL:             queueURL,
		workerID:             hostname,
		ffmpegTimeoutMinutes: ffmpegTimeoutMinutes,
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

	stopHeartbeat := startHeartbeat(ctx, p.sqsClient, p.queueURL, receiptHandle, p.db, msg.VideoID, heartbeatInterval)
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
	log.InfoContext(ctx, "baixando vídeo do S3", slog.String("bucket", msg.Bucket), slog.String("key", msg.S3Key))
	dlErr := downloadVideo(ctx, p.s3Client, msg.Bucket, msg.S3Key, inputPath)
	if dlErr != nil {
		dlSpan.RecordError(dlErr)
		dlSpan.SetStatus(codes.Error, dlErr.Error())
	}
	dlSpan.End()
	if dlErr != nil {
		return fmt.Errorf("falha ao baixar vídeo: %w", dlErr) // retentável
	}

	// ── FFmpeg ────────────────────────────────────────────────────────────────
	framesDir := filepath.Join(tempDir, "frames")
	if err := os.MkdirAll(framesDir, 0o700); err != nil {
		return fmt.Errorf("falha ao criar diretório de frames: %w", err)
	}

	log.InfoContext(ctx, "executando FFmpeg")
	ctx, ffSpan := observability.SpanWorker(ctx, "ffmpeg.execute")
	ffmpegStart := time.Now()
	frameCount, err := runFFmpeg(ctx, inputPath, framesDir, p.ffmpegTimeoutMinutes)
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
	log.InfoContext(ctx, "frames extraídos", slog.Int("frames", frameCount))
	observability.RecordFrameCount(ctx, int64(frameCount))

	// ── ZIP streaming + upload S3 ─────────────────────────────────────────────
	log.InfoContext(ctx, "fazendo upload do ZIP para S3", slog.String("bucket", msg.OutputBucket))
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
	log.InfoContext(ctx, "ZIP enviado", slog.String("key", outputKey))

	// ── Finalização em transação ──────────────────────────────────────────────
	if err := p.markDone(ctx, msg.VideoID, outputKey, frameCount); err != nil {
		return fmt.Errorf("falha ao finalizar vídeo no banco: %w", err) // retentável
	}

	// ── Notificação de sucesso (best-effort) ──────────────────────────────────
	if userEmail, err := p.getUserEmail(ctx, row.UserID); err == nil && userEmail != "" {
		p.notifier.SendSuccess(ctx, userEmail, msg.VideoID, row.OriginalName)
	}

	observability.RecordVideoProcessed(ctx, "done")
	observability.RecordVideoProcessingDuration(ctx, time.Since(start).Seconds(), "done")
	log.InfoContext(ctx, "processamento concluído", slog.String("output_key", outputKey))

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
