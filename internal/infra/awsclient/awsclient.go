package awsclient

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	appconfig "github.com/lukasqw/framecast-worker/internal/config"
)

type Clients struct {
	S3  *s3.Client
	SQS *sqs.Client
	SES *sesv2.Client
}

func New(ctx context.Context, cfg *appconfig.Config) (*Clients, error) {
	opts := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.AWSRegion),
	}

	if cfg.AWSAccessKeyID != "" && cfg.AWSSecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AWSAccessKeyID, cfg.AWSSecretAccessKey, cfg.AWSSessionToken),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("falha ao carregar config AWS: %w", err)
	}

	endpointURL := cfg.AWSEndpointURL

	s3Client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if endpointURL != "" {
			o.BaseEndpoint = aws.String(endpointURL)
			// Path-style obrigatório no LocalStack
			o.UsePathStyle = true
		}
	})

	sqsClient := sqs.NewFromConfig(awsCfg, func(o *sqs.Options) {
		if endpointURL != "" {
			o.BaseEndpoint = aws.String(endpointURL)
		}
	})

	sesClient := sesv2.NewFromConfig(awsCfg, func(o *sesv2.Options) {
		if endpointURL != "" {
			o.BaseEndpoint = aws.String(endpointURL)
		}
	})

	return &Clients{
		S3:  s3Client,
		SQS: sqsClient,
		SES: sesClient,
	}, nil
}
