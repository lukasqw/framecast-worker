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
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/lukasqw/framecast-worker/internal/consumer"
	"github.com/lukasqw/framecast-worker/internal/infra/observability"
	"gorm.io/gorm"
)

// Processor implementa consumer.Handler.
type Processor struct {
	db                   *gorm.DB
	s3Client             *s3.Client
	sqsClient            *sqs.Client
	queueURL             string
	workerID             string
	ffmpegTimeoutMinutes int
	notifier             *notifier
}

func New(
	db *gorm.DB,
	s3Client *s3.Client,
	sqsClient *sqs.Client,
	sesClient *sesv2.Client,
	queueURL, fromEmail, sesRecipientOverride string,
	ffmpegTimeoutMinutes int,
) *Processor {
	hostname, _ := os.Hostname()
	return &Processor{
		db:                   db,
		s3Client:             s3Client,
		sqsClient:            sqsClient,
		queueURL:             queueURL,
		workerID:             hostname,
		ffmpegTimeoutMinutes: ffmpegTimeoutMinutes,
		notifier:             newNotifier(sesClient, fromEmail, sesRecipientOverride),
	}
}

func (p *Processor) Process(ctx context.Context, msg *consumer.Message, receiptHandle string) error {
	log := slog.With(slog.String("video_id", msg.VideoID))
	start := time.Now()

	// ── Idempotência + lease ──────────────────────────────────────────────────
	row, err := acquireLease(ctx, p.db, msg.VideoID, p.workerID)
	if errors.Is(err, errAlreadyDone) || errors.Is(err, errAlreadyError) {
		log.Info("vídeo já finalizado — descartando mensagem SQS")
		return p.deleteMessage(ctx, receiptHandle)
	}
	if err != nil {
		return fmt.Errorf("falha ao adquirir lease: %w", err)
	}

	log.Info("lease adquirido", slog.Int("attempt", row.Attempt))

	stopHeartbeat := startHeartbeat(ctx, p.sqsClient, p.queueURL, receiptHandle)
	defer stopHeartbeat()

	// ── Diretório temporário — limpo sempre ao final ──────────────────────────
	tempDir := filepath.Join(os.TempDir(), msg.VideoID)
	if err := os.MkdirAll(tempDir, 0o700); err != nil {
		return fmt.Errorf("falha ao criar diretório temporário: %w", err)
	}
	defer os.RemoveAll(tempDir)

	// ── Download do vídeo original do S3 ─────────────────────────────────────
	inputPath := filepath.Join(tempDir, "input.mp4")
	log.Info("baixando vídeo do S3", slog.String("bucket", msg.Bucket), slog.String("key", msg.S3Key))
	if err := downloadVideo(ctx, p.s3Client, msg.Bucket, msg.S3Key, inputPath); err != nil {
		return fmt.Errorf("falha ao baixar vídeo: %w", err) // retentável
	}

	// ── FFmpeg ────────────────────────────────────────────────────────────────
	framesDir := filepath.Join(tempDir, "frames")
	if err := os.MkdirAll(framesDir, 0o700); err != nil {
		return fmt.Errorf("falha ao criar diretório de frames: %w", err)
	}

	log.Info("executando FFmpeg")
	ffmpegStart := time.Now()
	frameCount, err := runFFmpeg(ctx, inputPath, framesDir, p.ffmpegTimeoutMinutes)
	observability.RecordFFmpegDuration(ctx, time.Since(ffmpegStart).Seconds())

	if err != nil {
		// Não-retentável: vídeo corrompido, codec inválido ou timeout
		log.Error("FFmpeg falhou", slog.String("erro", err.Error()))
		userEmail, _ := p.getUserEmail(ctx, row.UserID)
		p.markError(ctx, msg.VideoID, err.Error())
		if userEmail != "" {
			p.notifier.sendFailure(ctx, userEmail, msg.VideoID, err.Error())
		}
		observability.RecordVideoProcessed(ctx, "error")
		return p.deleteMessage(ctx, receiptHandle)
	}

	log.Info("frames extraídos", slog.Int("frames", frameCount))
	observability.RecordFrameCount(ctx, int64(frameCount))

	// ── ZIP streaming + upload S3 ─────────────────────────────────────────────
	log.Info("fazendo upload do ZIP para S3", slog.String("bucket", msg.OutputBucket))
	outputKey, err := zipAndUpload(ctx, p.s3Client, framesDir, msg.OutputBucket, msg.VideoID)
	if err != nil {
		return fmt.Errorf("falha no zip+upload: %w", err) // retentável
	}
	log.Info("ZIP enviado", slog.String("key", outputKey))

	// ── Finalização em transação ──────────────────────────────────────────────
	if err := p.markDone(ctx, msg.VideoID, outputKey, frameCount); err != nil {
		return fmt.Errorf("falha ao finalizar vídeo no banco: %w", err) // retentável
	}

	// ── Notificação de sucesso (best-effort) ──────────────────────────────────
	if userEmail, err := p.getUserEmail(ctx, row.UserID); err == nil && userEmail != "" {
		p.notifier.sendSuccess(ctx, userEmail, msg.VideoID, row.OriginalName)
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
