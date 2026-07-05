package processor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func withFakeRunner(t *testing.T, fn commandRunner) {
	t.Helper()
	orig := runFFmpegRunner
	runFFmpegRunner = fn
	t.Cleanup(func() { runFFmpegRunner = orig })
}

func TestRunFFmpeg_Sucesso_ContaFrames(t *testing.T) {
	framesDir := t.TempDir()
	for _, name := range []string{"frame_0001.png", "frame_0002.png", "frame_0003.png"} {
		require.NoError(t, os.WriteFile(filepath.Join(framesDir, name), []byte("x"), 0o600))
	}

	var gotName string
	var gotArgs []string
	withFakeRunner(t, func(ctx context.Context, name string, args ...string) (string, error) {
		gotName = name
		gotArgs = args
		return "", nil
	})

	n, err := runFFmpeg(context.Background(), "/tmp/input.mp4", framesDir, 30, 1)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, "ffmpeg", gotName)
	assert.Contains(t, gotArgs, "/tmp/input.mp4")
	assert.Contains(t, gotArgs, "fps=1")
}

func TestRunFFmpeg_FPSConfiguravel(t *testing.T) {
	framesDir := t.TempDir()

	var gotArgs []string
	withFakeRunner(t, func(ctx context.Context, name string, args ...string) (string, error) {
		gotArgs = args
		return "", nil
	})

	_, err := runFFmpeg(context.Background(), "/tmp/input.mp4", framesDir, 30, 5)
	require.NoError(t, err)
	assert.Contains(t, gotArgs, "fps=5")
	assert.NotContains(t, gotArgs, "fps=1")
}

func TestRunFFmpeg_ErroNaoRetentavel(t *testing.T) {
	framesDir := t.TempDir()

	withFakeRunner(t, func(ctx context.Context, name string, args ...string) (string, error) {
		return "Invalid data found when processing input", errors.New("exit status 1")
	})

	_, err := runFFmpeg(context.Background(), "/tmp/corrupted.mp4", framesDir, 30, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid data found")
}

func TestRunFFmpeg_TruncaStderrLongo(t *testing.T) {
	framesDir := t.TempDir()
	longMsg := make([]byte, maxStderrBytes+200)
	for i := range longMsg {
		longMsg[i] = 'x'
	}

	withFakeRunner(t, func(ctx context.Context, name string, args ...string) (string, error) {
		return string(longMsg), errors.New("exit status 1")
	})

	_, err := runFFmpeg(context.Background(), "/tmp/input.mp4", framesDir, 30, 1)
	require.Error(t, err)
	assert.LessOrEqual(t, len(err.Error()), maxStderrBytes+len("ffmpeg: "))
}

func TestRunFFmpeg_Timeout(t *testing.T) {
	framesDir := t.TempDir()

	withFakeRunner(t, func(ctx context.Context, name string, args ...string) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})

	_, err := runFFmpeg(context.Background(), "/tmp/input.mp4", framesDir, 0, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timeout")
}

func TestCountFrames_IgnoraNaoPNG(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "frame_0001.png"), []byte("x"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o600))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "subdir.png"), 0o700))

	n, err := countFrames(dir)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

func TestCountFrames_DiretorioInexistente(t *testing.T) {
	_, err := countFrames(filepath.Join(t.TempDir(), "nao-existe"))
	require.Error(t, err)
}
