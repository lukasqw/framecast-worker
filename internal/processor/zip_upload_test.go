package processor

import (
	"archive/zip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeUploader struct {
	lastInput *s3.PutObjectInput
	err       error
}

func (f *fakeUploader) Upload(_ context.Context, input *s3.PutObjectInput, _ ...func(*manager.Uploader)) (*manager.UploadOutput, error) { //nolint:staticcheck // assinatura precisa casar com a interface uploader (zip_upload.go)
	f.lastInput = input
	// Drena o pipe por completo — reproduz o caminho single-part (ZIP <= 5MiB, o
	// PartSize padrão) do s3manager.Uploader real, que lê o body inteiro antes de
	// decidir upload single vs multipart e só então chama PutObject. No caminho
	// multipart (ZIP maior) o uploader real NÃO garante essa drenagem quando falha
	// (ver fakeUploaderNoDrain / TestZipAndUpload_ErroSemDrenarPipe_NaoTrava).
	_, copyErr := io.Copy(io.Discard, input.Body)
	if f.err != nil {
		return nil, f.err
	}
	if copyErr != nil {
		return nil, copyErr
	}
	return &manager.UploadOutput{}, nil
}

// fakeUploaderNoDrain reproduz o caminho multipart do s3manager.Uploader real
// quando CreateMultipartUpload (ou uma parte) falha antes de esgotar o body —
// o body não é lido. Ver aws-sdk-go-v2/feature/s3/manager upload.go:663-668 e
// 683-703 (o loop de leitura para assim que u.geterr() != nil).
type fakeUploaderNoDrain struct{ err error }

func (f *fakeUploaderNoDrain) Upload(_ context.Context, _ *s3.PutObjectInput, _ ...func(*manager.Uploader)) (*manager.UploadOutput, error) { //nolint:staticcheck // assinatura precisa casar com a interface uploader (zip_upload.go)
	return nil, f.err
}

func writeFakeFrames(t *testing.T, dir string, names ...string) {
	t.Helper()
	for _, name := range names {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("png-bytes-"+name), 0o600))
	}
}

// runWriteFramesToZip replica o padrão de uso real (zipAndUpload): fecha o pipe
// após writeFramesToZip retornar — a função em si só fecha o zip.Writer, não o pipe.
func runWriteFramesToZip(pw *io.PipeWriter, framesDir string) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		err := writeFramesToZip(pw, framesDir)
		if err != nil {
			pw.CloseWithError(err)
		} else {
			pw.Close()
		}
		errCh <- err
	}()
	return errCh
}

func TestWriteFramesToZip_ContemTodosOsPNGs(t *testing.T) {
	dir := t.TempDir()
	writeFakeFrames(t, dir, "frame_0001.png", "frame_0002.png")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ignore.txt"), []byte("x"), 0o600))

	pr, pw := io.Pipe()
	errCh := runWriteFramesToZip(pw, dir)

	data, readErr := io.ReadAll(pr)
	require.NoError(t, readErr)
	require.NoError(t, <-errCh)

	zr, err := zip.NewReader(bytesReaderAt(data), int64(len(data)))
	require.NoError(t, err)

	var names []string
	for _, f := range zr.File {
		names = append(names, f.Name)
	}
	assert.ElementsMatch(t, []string{"frame_0001.png", "frame_0002.png"}, names)
}

func TestWriteFramesToZip_DiretorioInexistente(t *testing.T) {
	pr, pw := io.Pipe()
	errCh := runWriteFramesToZip(pw, filepath.Join(t.TempDir(), "nao-existe"))

	_, _ = io.ReadAll(pr)
	require.Error(t, <-errCh)
}

func TestZipAndUpload_Sucesso(t *testing.T) {
	dir := t.TempDir()
	writeFakeFrames(t, dir, "frame_0001.png")

	up := &fakeUploader{}
	key, err := zipAndUpload(context.Background(), up, dir, "output-bucket", "video-123")
	require.NoError(t, err)
	assert.Equal(t, "video-123.zip", key)
	require.NotNil(t, up.lastInput)
	assert.Equal(t, "output-bucket", *up.lastInput.Bucket)
}

func TestZipAndUpload_ErroDeUpload(t *testing.T) {
	dir := t.TempDir()
	writeFakeFrames(t, dir, "frame_0001.png")

	up := &fakeUploader{err: errors.New("s3 indisponível")}
	_, err := zipAndUpload(context.Background(), up, dir, "output-bucket", "video-123")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "upload")
}

// TestZipAndUpload_ErroSemDrenarPipe_NaoTrava reproduz o caso em que o upload S3
// falha (ex.: CreateMultipartUpload sem permissão) sem nunca ler o body — sem o
// pr.CloseWithError em zipAndUpload, a goroutine produtora travaria para sempre
// em pw.Write() e este teste travaria até o timeout do `go test`.
func TestZipAndUpload_ErroSemDrenarPipe_NaoTrava(t *testing.T) {
	dir := t.TempDir()
	writeFakeFrames(t, dir, "frame_0001.png")

	up := &fakeUploaderNoDrain{err: errors.New("create multipart upload falhou")}

	type result struct {
		key string
		err error
	}
	done := make(chan result, 1)
	go func() {
		key, err := zipAndUpload(context.Background(), up, dir, "output-bucket", "video-123")
		done <- result{key: key, err: err}
	}()

	select {
	case res := <-done:
		require.Error(t, res.err)
		assert.Contains(t, res.err.Error(), "upload")
	case <-time.After(2 * time.Second):
		t.Fatal("zipAndUpload travou quando o uploader falhou sem drenar o pipe")
	}
}

func TestZipAndUpload_ErroAoListarFrames(t *testing.T) {
	up := &fakeUploader{}
	_, err := zipAndUpload(context.Background(), up, filepath.Join(t.TempDir(), "nao-existe"), "output-bucket", "video-123")
	require.Error(t, err)
}

// bytesReaderAt adapta um []byte para io.ReaderAt, necessário por zip.NewReader.
type bytesReaderAtImpl struct{ b []byte }

func (r bytesReaderAtImpl) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(r.b)) {
		return 0, io.EOF
	}
	n := copy(p, r.b[off:])
	return n, nil
}

func bytesReaderAt(b []byte) io.ReaderAt { return bytesReaderAtImpl{b: b} }
