package processor

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/lukasqw/framecast-worker/internal/consumer"
	"gorm.io/gorm"
)

const dlqErrorReason = "Processamento falhou após múltiplas tentativas (mensagem movida para a DLQ)."

// DLQHandler consome a fila de dead-letter: marca o vídeo como ERROR e notifica o
// usuário por e-mail (best-effort), fechando o requisito de "notificação de erro"
// para falhas de infraestrutura que esgotaram o maxReceiveCount.
type DLQHandler struct {
	db       *gorm.DB
	sqs      sqsAPI
	queueURL string
	notifier *notifier
}

func NewDLQHandler(db *gorm.DB, sqsClient sqsAPI, sesClient sesAPI, dlqURL, fromEmail, recipientOverride string) *DLQHandler {
	return &DLQHandler{
		db:       db,
		sqs:      sqsClient,
		queueURL: dlqURL,
		notifier: newNotifier(sesClient, fromEmail, recipientOverride),
	}
}

func (h *DLQHandler) Process(ctx context.Context, msg *consumer.Message, receiptHandle string) error {
	log := slog.With(slog.String("video_id", msg.VideoID), slog.String("origem", "dlq"))

	// Marca ERROR apenas se ainda não finalizado, capturando o e-mail do dono na mesma TX.
	var userEmail string
	var alreadyFinal bool
	err := h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var row struct {
			Status string
			UserID string
		}
		if e := tx.Raw(
			`SELECT status, user_id FROM videos WHERE id = ? FOR UPDATE`, msg.VideoID,
		).Scan(&row).Error; e != nil {
			return e
		}
		if row.Status == "DONE" || row.Status == "ERROR" {
			alreadyFinal = true
			return nil
		}
		if e := tx.Exec(
			`UPDATE videos SET status='ERROR', error_message=?, worker_id=NULL, updated_at=NOW() WHERE id=?`,
			dlqErrorReason, msg.VideoID,
		).Error; e != nil {
			return e
		}
		return tx.Raw(`SELECT email FROM users WHERE id = ?`, row.UserID).Scan(&userEmail).Error
	})
	if err != nil {
		// Não deleta — DLQ reentrega; preferível a perder o sinal de erro.
		return fmt.Errorf("dlq: falha ao marcar ERROR: %w", err)
	}

	if alreadyFinal {
		log.Info("vídeo já finalizado — apenas descartando mensagem da DLQ")
	} else {
		if userEmail != "" {
			h.notifier.sendFailure(ctx, userEmail, msg.VideoID, dlqErrorReason)
		}
		log.Info("vídeo marcado como ERROR via DLQ e usuário notificado")
	}

	if _, derr := h.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(h.queueURL),
		ReceiptHandle: aws.String(receiptHandle),
	}); derr != nil {
		return fmt.Errorf("dlq: falha ao deletar mensagem: %w", derr)
	}
	return nil
}
