package processor

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSES struct {
	lastInput *sesv2.SendEmailInput
	err       error
}

func (f *fakeSES) SendEmail(_ context.Context, params *sesv2.SendEmailInput, _ ...func(*sesv2.Options)) (*sesv2.SendEmailOutput, error) {
	f.lastInput = params
	if f.err != nil {
		return nil, f.err
	}
	return &sesv2.SendEmailOutput{}, nil
}

func TestNotifier_SendSuccess_EnviaParaDestinatario(t *testing.T) {
	ses := &fakeSES{}
	n := newNotifier(ses, "noreply@framecast.local", "")

	n.sendSuccess(context.Background(), "user@example.com", "v1", "video.mp4")

	require.NotNil(t, ses.lastInput)
	assert.Equal(t, "noreply@framecast.local", aws.ToString(ses.lastInput.FromEmailAddress))
	assert.Equal(t, []string{"user@example.com"}, ses.lastInput.Destination.ToAddresses)
}

func TestNotifier_SendFailure_EnviaParaDestinatario(t *testing.T) {
	ses := &fakeSES{}
	n := newNotifier(ses, "noreply@framecast.local", "")

	n.sendFailure(context.Background(), "user@example.com", "v1", "ffmpeg falhou")

	require.NotNil(t, ses.lastInput)
	assert.Equal(t, []string{"user@example.com"}, ses.lastInput.Destination.ToAddresses)
}

func TestNotifier_RecipientOverride_RedirecionaEmail(t *testing.T) {
	ses := &fakeSES{}
	n := newNotifier(ses, "noreply@framecast.local", "dev-catchall@example.com")

	n.sendSuccess(context.Background(), "user@example.com", "v1", "video.mp4")

	require.NotNil(t, ses.lastInput)
	assert.Equal(t, []string{"dev-catchall@example.com"}, ses.lastInput.Destination.ToAddresses)
}

func TestNotifier_ErroDoSES_NaoPropaga(t *testing.T) {
	ses := &fakeSES{err: errors.New("ses indisponível")}
	n := newNotifier(ses, "noreply@framecast.local", "")

	// best-effort: não deve panicar nem retornar erro (sem valor de retorno)
	n.sendSuccess(context.Background(), "user@example.com", "v1", "video.mp4")
	n.sendFailure(context.Background(), "user@example.com", "v1", "erro qualquer")
}
