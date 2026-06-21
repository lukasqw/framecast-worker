package processor

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"
)

const maxStderrBytes = 500

// commandRunner executa um comando externo e retorna o stderr capturado.
// Seam de teste: runFFmpegRunner pode ser substituída por um fake sem precisar
// do binário ffmpeg instalado.
type commandRunner func(ctx context.Context, name string, args ...string) (stderr string, err error)

var runFFmpegRunner commandRunner = func(ctx context.Context, name string, args ...string) (string, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stderr = &stderr
	err := cmd.Run()
	return stderr.String(), err
}

// runFFmpeg executa o FFmpeg para extrair 1 frame por segundo do inputPath,
// salvando os PNGs em framesDir com o padrão frame_%04d.png.
// Retorna a contagem de frames gerados ou erro não-retentável.
func runFFmpeg(ctx context.Context, inputPath, framesDir string, timeoutMinutes int) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutMinutes)*time.Minute)
	defer cancel()

	outputPattern := filepath.Join(framesDir, "frame_%04d.png")

	stderr, err := runFFmpegRunner(ctx, "ffmpeg",
		"-i", inputPath,
		"-vf", "fps=1",
		"-loglevel", "error",
		outputPattern,
	)
	if err != nil {
		if ctx.Err() != nil {
			return 0, fmt.Errorf("timeout atingido após %d minutos", timeoutMinutes)
		}
		if len(stderr) > maxStderrBytes {
			stderr = stderr[:maxStderrBytes]
		}
		return 0, fmt.Errorf("ffmpeg: %s", stderr)
	}

	return countFrames(framesDir)
}

func countFrames(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("falha ao listar frames em %s: %w", dir, err)
	}
	n := 0
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".png" {
			n++
		}
	}
	return n, nil
}
