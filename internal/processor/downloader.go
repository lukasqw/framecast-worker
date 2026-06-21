package processor

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// downloadVideo faz streaming do objeto S3 diretamente para destPath em disco.
func downloadVideo(ctx context.Context, s3Client s3GetAPI, bucket, key, destPath string) error {
	out, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("falha ao obter s3://%s/%s: %w", bucket, key, err)
	}
	defer out.Body.Close()

	f, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("falha ao criar arquivo de destino %s: %w", destPath, err)
	}
	defer f.Close()

	if _, err := io.Copy(f, out.Body); err != nil {
		return fmt.Errorf("falha ao escrever vídeo em disco: %w", err)
	}
	return nil
}
