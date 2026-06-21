package processor

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeS3Get struct {
	body string
	err  error
}

func (f *fakeS3Get) GetObject(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(f.body))}, nil
}

func TestDownloadVideo_Sucesso(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "input.mp4")
	client := &fakeS3Get{body: "conteudo-do-video"}

	err := downloadVideo(context.Background(), client, "raw", "u1/v1/orig", dest)
	require.NoError(t, err)

	data, err := os.ReadFile(dest)
	require.NoError(t, err)
	assert.Equal(t, "conteudo-do-video", string(data))
}

func TestDownloadVideo_ErroNoS3(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "input.mp4")
	client := &fakeS3Get{err: errors.New("NoSuchKey")}

	err := downloadVideo(context.Background(), client, "raw", "u1/v1/orig", dest)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "raw")
}

func TestDownloadVideo_ErroAoCriarArquivoDestino(t *testing.T) {
	client := &fakeS3Get{body: "x"}
	// diretório inexistente como "arquivo" de destino força erro em os.Create
	dest := filepath.Join(t.TempDir(), "nao-existe", "input.mp4")

	err := downloadVideo(context.Background(), client, "raw", "u1/v1/orig", dest)
	require.Error(t, err)
}
