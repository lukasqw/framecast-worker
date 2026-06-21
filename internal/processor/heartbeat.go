package processor

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"gorm.io/gorm"
)

const heartbeatExtension = int32(15 * 60) // estende visibilidade para 15 min

// heartbeatInterval é var (não const) para permitir interval curto nos testes.
var heartbeatInterval = 1 * time.Minute // renova lease no banco e visibility no SQS

// startHeartbeat lança uma goroutine que, a cada heartbeatInterval:
//   - renova a visibilidade da mensagem SQS (evita reentrega enquanto processa);
//   - renova o lease no banco (last_heartbeat_at = NOW()), sinalizando que o worker
//     está vivo — usado pelo acquireLease de outros workers (P1-6).
//
// Retorna uma função de cancelamento — chamar via defer ao fim do processamento.
func startHeartbeat(ctx context.Context, sqsClient sqsAPI, queueURL, receiptHandle string, db *gorm.DB, videoID string) context.CancelFunc {
	hbCtx, cancel := context.WithCancel(ctx)

	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				if _, err := sqsClient.ChangeMessageVisibility(hbCtx, &sqs.ChangeMessageVisibilityInput{
					QueueUrl:          aws.String(queueURL),
					ReceiptHandle:     aws.String(receiptHandle),
					VisibilityTimeout: heartbeatExtension,
				}); err != nil {
					slog.Error("falha no heartbeat SQS — visibility não renovada",
						slog.String("erro", err.Error()),
					)
				}

				if err := db.WithContext(hbCtx).Exec(
					`UPDATE videos SET last_heartbeat_at = NOW() WHERE id = ?`, videoID,
				).Error; err != nil {
					slog.Error("falha no heartbeat do lease — last_heartbeat_at não renovado",
						slog.String("video_id", videoID),
						slog.String("erro", err.Error()),
					)
				}
			}
		}
	}()

	return cancel
}
