package email

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	sestypes "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
)

// sesAPI é o subconjunto do *sesv2.Client usado pelo SESNotifier.
type sesAPI interface {
	SendEmail(ctx context.Context, params *sesv2.SendEmailInput, optFns ...func(*sesv2.Options)) (*sesv2.SendEmailOutput, error)
}

// SESNotifier envia notificações via AWS SES v2.
// Mantido desabilitado na AWS Academy (LabRole bloqueia ses:VerifyEmailIdentity).
// Habilitar via NOTIFIER_BACKEND=ses em ambientes com SES configurado.
type SESNotifier struct {
	ses               sesAPI
	fromEmail         string
	recipientOverride string // dev/staging: redireciona todos os e-mails para este endereço
}

func NewSESNotifier(sesClient sesAPI, fromEmail, recipientOverride string) *SESNotifier {
	return &SESNotifier{ses: sesClient, fromEmail: fromEmail, recipientOverride: recipientOverride}
}

func (n *SESNotifier) SendSuccess(ctx context.Context, toEmail, videoID, filename, downloadURL string) {
	subject, html, text, err := buildSuccessEmail(filename, videoID, downloadURL)
	if err != nil {
		slog.ErrorContext(ctx, "falha ao montar e-mail de sucesso (best-effort)",
			slog.String("video_id", videoID), slog.String("erro", err.Error()))
		return
	}
	if err := n.send(ctx, toEmail, subject, html, text); err != nil {
		slog.ErrorContext(ctx, "falha ao enviar e-mail de sucesso via SES (best-effort)",
			slog.String("video_id", videoID),
			slog.String("erro", err.Error()),
		)
	}
}

func (n *SESNotifier) SendFailure(ctx context.Context, toEmail, videoID, errMsg string) {
	subject, html, text, err := buildFailureEmail(videoID, errMsg)
	if err != nil {
		slog.ErrorContext(ctx, "falha ao montar e-mail de falha (best-effort)",
			slog.String("video_id", videoID), slog.String("erro", err.Error()))
		return
	}
	if err := n.send(ctx, toEmail, subject, html, text); err != nil {
		slog.ErrorContext(ctx, "falha ao enviar e-mail de erro via SES (best-effort)",
			slog.String("video_id", videoID),
			slog.String("erro", err.Error()),
		)
	}
}

func (n *SESNotifier) send(ctx context.Context, toEmail, subject, html, text string) error {
	recipient := toEmail
	if n.recipientOverride != "" {
		recipient = n.recipientOverride
	}
	_, err := n.ses.SendEmail(ctx, &sesv2.SendEmailInput{
		FromEmailAddress: aws.String(n.fromEmail),
		Destination: &sestypes.Destination{
			ToAddresses: []string{recipient},
		},
		Content: &sestypes.EmailContent{
			Simple: &sestypes.Message{
				Subject: &sestypes.Content{Data: aws.String(subject)},
				Body: &sestypes.Body{
					Html: &sestypes.Content{Data: aws.String(html)},
					Text: &sestypes.Content{Data: aws.String(text)},
				},
			},
		},
	})
	return err
}
