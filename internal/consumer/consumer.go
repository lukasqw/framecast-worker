package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	visibilityTimeoutSec = 15 * 60 // 15 min
	waitTimeSeconds      = 20
	maxMessages          = 1
)

// Message é o payload publicado pelo outbox dispatcher da api.
type Message struct {
	VideoID      string `json:"video_id"`
	UserID       string `json:"user_id"`
	S3Key        string `json:"s3_key"`
	Bucket       string `json:"bucket"`
	OutputBucket string `json:"output_bucket"`
}

// Handler processa uma mensagem já parseada. Implementado pelo processor.
type Handler interface {
	Process(ctx context.Context, msg *Message, receiptHandle string) error
}

// sqsAPI é o subconjunto do *sqs.Client usado pelo consumer. *sqs.Client satisfaz
// essa interface automaticamente — só os testes precisam de um fake.
type sqsAPI interface {
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
}

type Consumer struct {
	sqs         sqsAPI
	queueURL    string
	concurrency int
	handler     Handler
}

func New(sqsClient sqsAPI, queueURL string, concurrency int, handler Handler) *Consumer {
	return &Consumer{
		sqs:         sqsClient,
		queueURL:    queueURL,
		concurrency: concurrency,
		handler:     handler,
	}
}

// Run inicia o loop de consumo. Bloqueia até ctx ser cancelado.
// Aguarda todas as goroutines de processamento finalizarem antes de retornar.
func (c *Consumer) Run(ctx context.Context) {
	sem := make(chan struct{}, c.concurrency)
	var wg sync.WaitGroup

	slog.Info("consumer SQS iniciado",
		slog.String("fila", c.queueURL),
		slog.Int("concorrência", c.concurrency),
	)

	for {
		// Verifica cancelamento antes de bloquear no receive
		select {
		case <-ctx.Done():
			wg.Wait()
			slog.Info("consumer encerrado")
			return
		default:
		}

		msgs, err := c.receive(ctx)
		if err != nil {
			// ctx cancelado durante long polling — encerra limpo
			if ctx.Err() != nil {
				wg.Wait()
				return
			}
			slog.Error("erro ao receber mensagens SQS", slog.String("erro", err.Error()))
			continue
		}

		for _, m := range msgs {
			msg, err := parse(m)
			if err != nil {
				slog.Error("mensagem SQS inválida — descartando",
					slog.String("erro", err.Error()),
					slog.String("receipt", aws.ToString(m.ReceiptHandle)),
				)
				c.delete(ctx, m.ReceiptHandle)
				continue
			}

			sem <- struct{}{}
			wg.Add(1)
			go func(sqsMsg sqstypes.Message, parsed *Message) {
				defer wg.Done()
				defer func() { <-sem }()

				if err := c.handler.Process(ctx, parsed, aws.ToString(sqsMsg.ReceiptHandle)); err != nil {
					slog.Error("erro ao processar mensagem",
						slog.String("video_id", parsed.VideoID),
						slog.String("erro", err.Error()),
					)
				}
			}(m, msg)
		}
	}
}

func (c *Consumer) receive(ctx context.Context) ([]sqstypes.Message, error) {
	out, err := c.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(c.queueURL),
		MaxNumberOfMessages: maxMessages,
		WaitTimeSeconds:     waitTimeSeconds,
		VisibilityTimeout:   visibilityTimeoutSec,
	})
	if err != nil {
		return nil, err
	}
	return out.Messages, nil
}

func (c *Consumer) delete(ctx context.Context, receiptHandle *string) {
	if receiptHandle == nil {
		return
	}
	if _, err := c.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(c.queueURL),
		ReceiptHandle: receiptHandle,
	}); err != nil {
		slog.Error("falha ao deletar mensagem inválida do SQS",
			slog.String("erro", err.Error()),
		)
	}
}

func parse(m sqstypes.Message) (*Message, error) {
	if m.Body == nil {
		return nil, nil
	}
	var msg Message
	if err := json.Unmarshal([]byte(*m.Body), &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
