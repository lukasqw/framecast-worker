package email

import (
	"context"
	"fmt"
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

func (n *SESNotifier) SendSuccess(ctx context.Context, toEmail, videoID, filename string) {
	subject := "Seu vídeo está pronto para download"
	body := fmt.Sprintf(
		"Olá!\n\nSeu vídeo %q foi processado com sucesso.\n"+
			"Acesse a plataforma para baixar os frames extraídos.\n\n"+
			"ID do vídeo: %s",
		filename, videoID,
	)
	if err := n.send(ctx, toEmail, subject, body); err != nil {
		slog.ErrorContext(ctx, "falha ao enviar e-mail de sucesso via SES (best-effort)",
			slog.String("video_id", videoID),
			slog.String("erro", err.Error()),
		)
	}
}

func (n *SESNotifier) SendFailure(ctx context.Context, toEmail, videoID, errMsg string) {
	subject := "Falha no processamento do seu vídeo"
	body := fmt.Sprintf(
		"Olá!\n\nOcorreu um erro ao processar seu vídeo.\n\n"+
			"ID do vídeo: %s\nMotivo: %s\n\n"+
			"Entre em contato se precisar de ajuda.",
		videoID, errMsg,
	)
	if err := n.send(ctx, toEmail, subject, body); err != nil {
		slog.ErrorContext(ctx, "falha ao enviar e-mail de erro via SES (best-effort)",
			slog.String("video_id", videoID),
			slog.String("erro", err.Error()),
		)
	}
}

func (n *SESNotifier) send(ctx context.Context, toEmail, subject, body string) error {
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
					Text: &sestypes.Content{Data: aws.String(body)},
				},
			},
		},
	})
	return err
}
