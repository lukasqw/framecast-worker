package email

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── SMTPNotifier ─────────────────────────────────────────────────────────────

func TestSMTPNotifier_DevMode_SemCredenciais_NaoErra(t *testing.T) {
	// Username vazio → dev mode: imprime no stdout, não tenta conexão.
	n := NewSMTPNotifier("localhost", "2525", "", "", "noreply@test.local")

	// best-effort: não deve panicar nem retornar erro
	n.SendSuccess(context.Background(), "user@example.com", "v1", "video.mp4")
	n.SendFailure(context.Background(), "user@example.com", "v1", "ffmpeg error")
}

func TestSMTPNotifier_ErroSMTP_NaoPropaga(t *testing.T) {
	// Host inválido → smtp.SendMail retorna erro, mas SendSuccess/Failure swallows.
	n := NewSMTPNotifier("invalid.host.local", "9999", "user", "pass", "noreply@test.local")

	// best-effort: não deve panicar
	n.SendSuccess(context.Background(), "user@example.com", "v1", "video.mp4")
	n.SendFailure(context.Background(), "user@example.com", "v1", "ffmpeg error")
}

// ── NoOpNotifier ─────────────────────────────────────────────────────────────

func TestNoOpNotifier_NaoFazNada(t *testing.T) {
	n := NoOpNotifier{}
	// Não deve panicar
	n.SendSuccess(context.Background(), "a@b.com", "v1", "f.mp4")
	n.SendFailure(context.Background(), "a@b.com", "v1", "error")
}

// ── MockNotifier ─────────────────────────────────────────────────────────────

func TestMockNotifier_CapturaSuccessEFailure(t *testing.T) {
	m := &MockNotifier{}
	m.SendSuccess(context.Background(), "a@b.com", "v1", "f.mp4")
	m.SendSuccess(context.Background(), "b@b.com", "v2", "g.mp4")
	m.SendFailure(context.Background(), "c@b.com", "v3", "err")

	require.Len(t, m.SuccessCalls, 2)
	assert.Equal(t, "a@b.com", m.SuccessCalls[0].ToEmail)
	assert.Equal(t, "f.mp4", m.SuccessCalls[0].Detail)

	require.Len(t, m.FailureCalls, 1)
	assert.Equal(t, "c@b.com", m.FailureCalls[0].ToEmail)
	assert.Equal(t, "err", m.FailureCalls[0].Detail)
}

func TestMockNotifier_Reset_LimpaHistorico(t *testing.T) {
	m := &MockNotifier{}
	m.SendSuccess(context.Background(), "a@b.com", "v1", "f.mp4")
	m.SendFailure(context.Background(), "a@b.com", "v1", "err")
	m.Reset()

	assert.Empty(t, m.SuccessCalls)
	assert.Empty(t, m.FailureCalls)
}
