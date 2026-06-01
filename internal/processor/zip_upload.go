package processor

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// zipAndUpload faz streaming do ZIP diretamente para S3 via io.Pipe, sem
// materializar o arquivo em disco.
//
// Goroutine A: itera os frames PNG em framesDir → zip.Writer → pipe.Writer
// Goroutine B (inline): s3manager.Uploader lê do pipe.Reader → S3 PutObject
//
// Retorna a chave S3 do ZIP gerado (ex.: "<videoID>.zip").
func zipAndUpload(ctx context.Context, s3Client *s3.Client, framesDir, bucket, videoID string) (string, error) {
	outputKey := videoID + ".zip"
	pr, pw := io.Pipe()

	zipErrCh := make(chan error, 1)
	go func() {
		err := writeFramesToZip(pw, framesDir)
		if err != nil {
			// Propaga o erro para o leitor (uploader) — cancela o upload
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
		zipErrCh <- err
	}()

	uploader := manager.NewUploader(s3Client)
	_, uploadErr := uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(outputKey),
		Body:        pr,
		ContentType: aws.String("application/zip"),
	})

	zipErr := <-zipErrCh

	if uploadErr != nil {
		return "", fmt.Errorf("falha no upload S3 do ZIP: %w", uploadErr)
	}
	if zipErr != nil {
		return "", fmt.Errorf("falha ao criar ZIP: %w", zipErr)
	}

	return outputKey, nil
}

// writeFramesToZip escreve todos os PNGs de framesDir no zip.Writer apontado para w.
func writeFramesToZip(w *io.PipeWriter, framesDir string) error {
	zw := zip.NewWriter(w)

	entries, err := os.ReadDir(framesDir)
	if err != nil {
		return fmt.Errorf("falha ao listar frames: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".png" {
			continue
		}

		f, err := os.Open(filepath.Join(framesDir, entry.Name()))
		if err != nil {
			return fmt.Errorf("falha ao abrir frame %s: %w", entry.Name(), err)
		}

		ze, err := zw.Create(entry.Name())
		if err != nil {
			f.Close()
			return fmt.Errorf("falha ao criar entrada ZIP para %s: %w", entry.Name(), err)
		}

		_, copyErr := io.Copy(ze, f)
		f.Close()
		if copyErr != nil {
			return fmt.Errorf("falha ao copiar %s para ZIP: %w", entry.Name(), copyErr)
		}
	}

	// zw.Close() escreve o diretório central do ZIP — erro não ignorado
	return zw.Close()
}
