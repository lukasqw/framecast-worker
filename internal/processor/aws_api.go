package processor

import (
	"context"

	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// s3API é o subconjunto do *s3.Client usado pelo processor: download (GetObject)
// e upload do ZIP via s3manager (manager.UploadAPIClient). *s3.Client satisfaz essa
// interface automaticamente — só os testes precisam de um fake.
type s3API interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	manager.UploadAPIClient
}

// sqsAPI é o subconjunto do *sqs.Client usado pelo processor (ACK + heartbeat) e
// pelo DLQ handler (ACK).
type sqsAPI interface {
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error)
}

// s3GetAPI é o subconjunto usado só pelo downloader.
type s3GetAPI interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// presignAPI é o subconjunto do *s3.PresignClient usado para gerar a URL de
// download do ZIP incluída no e-mail de sucesso (best-effort — ver Processor.Process).
type presignAPI interface {
	PresignGetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error)
}
