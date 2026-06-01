package processor

import (
	"context"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

const (
	heartbeatInterval  = 5 * time.Minute
	heartbeatExtension = int32(15 * 60) // estende visibilidade para 15 min
)

// startHeartbeat lança uma goroutine que renova a visibilidade da mensagem SQS
// a cada 5 minutos. Retorna uma função de cancelamento — deve ser chamada via defer
// assim que o processamento terminar (sucesso ou erro).
func startHeartbeat(ctx context.Context, sqsClient *sqs.Client, queueURL, receiptHandle string) context.CancelFunc {
	hbCtx, cancel := context.WithCancel(ctx)

	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				_, err := sqsClient.ChangeMessageVisibility(hbCtx, &sqs.ChangeMessageVisibilityInput{
					QueueUrl:          aws.String(queueURL),
					ReceiptHandle:     aws.String(receiptHandle),
					VisibilityTimeout: heartbeatExtension,
				})
				if err != nil {
					slog.Error("falha no heartbeat SQS — visibility não renovada",
						slog.String("erro", err.Error()),
					)
				}
			}
		}
	}()

	return cancel
}
