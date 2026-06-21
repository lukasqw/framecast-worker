package processor

import (
	"archive/zip"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

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
	// Sempre drena o pipe — como o s3manager.Uploader real, que lê o body por
	// completo mesmo quando o upload acaba falhando. Sem isso, o produtor
	// (writeFramesToZip) bloquearia para sempre escrevendo num pipe sem leitor.
	_, copyErr := io.Copy(io.Discard, input.Body)
	if f.err != nil {
		return nil, f.err
	}
	if copyErr != nil {
		return nil, copyErr
	}
	return &manager.UploadOutput{}, nil
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
