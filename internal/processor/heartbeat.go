package processor

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/lukasqw/framecast-worker/internal/infra/observability"
	"gorm.io/gorm"
)

const heartbeatExtension = int32(15 * 60) // estende visibilidade para 15 min

// heartbeatInterval é var (não const) para permitir interval curto nos testes.
var heartbeatInterval = 1 * time.Minute

// maxConsecutiveSQSErrors: após N falhas consecutivas de ChangeMessageVisibility,
// a visibility do SQS expira e a mensagem será reentregue — abandonamos o
// processamento para evitar trabalho duplicado não coordenado.
const maxConsecutiveSQSErrors = 3

// startHeartbeat lança uma goroutine que, a cada interval:
//   - renova a visibilidade da mensagem SQS (evita reentrega enquanto processa);
//   - renova o lease no banco (last_heartbeat_at = NOW()), sinalizando que o worker
//     está vivo — usado pelo acquireLease de outros workers (P1-6).
//
// Retorna (cancelFunc, errCh). errCh recebe um erro quando o heartbeat SQS falha
// consecutivamente (maxConsecutiveSQSErrors vezes) ou a goroutine sofre um panic —
// sinal para o processador parar sem chamar DeleteMessage, deixando o SQS reentregar.
// Chamar cancelFunc via defer encerra a goroutine normalmente.
func startHeartbeat(ctx context.Context, sqsClient sqsAPI, queueURL, receiptHandle string, db *gorm.DB, videoID string, interval time.Duration) (context.CancelFunc, <-chan error) {
	hbCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				observability.RecordHeartbeatPanic(hbCtx)
				slog.ErrorContext(hbCtx, "panic na goroutine de heartbeat — encerrada",
					slog.Any("recover", r),
					slog.String("video_id", videoID),
				)
				select {
				case errCh <- fmt.Errorf("panic no heartbeat: %v", r):
				default:
				}
			}
		}()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		var consecutiveSQSErrors int

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
					observability.RecordHeartbeatSQSError(hbCtx)
					slog.ErrorContext(hbCtx, "falha no heartbeat SQS — visibility não renovada",
						slog.String("video_id", videoID),
						slog.String("erro", err.Error()),
					)
					consecutiveSQSErrors++
					if consecutiveSQSErrors >= maxConsecutiveSQSErrors {
						observability.RecordProcessingAbandoned(hbCtx)
						slog.ErrorContext(hbCtx, "heartbeat SQS falhou repetidamente — processamento será abandonado para SQS reentregar",
							slog.String("video_id", videoID),
							slog.Int("consecutive_errors", consecutiveSQSErrors),
						)
						select {
						case errCh <- fmt.Errorf("heartbeat SQS falhou %d vezes consecutivas", consecutiveSQSErrors):
						default:
						}
						return
					}
				} else {
					consecutiveSQSErrors = 0
				}

				if err := db.WithContext(hbCtx).Exec(
					`UPDATE videos SET last_heartbeat_at = NOW() WHERE id = ?`, videoID,
				).Error; err != nil {
					observability.RecordHeartbeatDBError(hbCtx)
					slog.ErrorContext(hbCtx, "falha no heartbeat do lease — last_heartbeat_at não renovado",
						slog.String("video_id", videoID),
						slog.String("erro", err.Error()),
					)
				}
			}
		}
	}()

	return cancel, errCh
}
