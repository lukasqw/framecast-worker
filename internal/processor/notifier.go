package processor

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	sestypes "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
)

type notifier struct {
	ses               sesAPI
	fromEmail         string
	recipientOverride string // dev: força todos os e-mails para um endereço fixo
}

func newNotifier(sesClient sesAPI, fromEmail, recipientOverride string) *notifier {
	return &notifier{ses: sesClient, fromEmail: fromEmail, recipientOverride: recipientOverride}
}

// sendSuccess envia e-mail de conclusão — best-effort, falha apenas logada.
func (n *notifier) sendSuccess(ctx context.Context, toEmail, videoID, filename string) {
	subject := "Seu vídeo está pronto para download"
	body := fmt.Sprintf(
		"Olá!\n\nSeu vídeo %q foi processado com sucesso.\n"+
			"Acesse a plataforma para baixar os frames extraídos.\n\n"+
			"ID do vídeo: %s",
		filename, videoID,
	)
	if err := n.send(ctx, toEmail, subject, body); err != nil {
		slog.Error("falha ao enviar e-mail de sucesso (best-effort)",
			slog.String("video_id", videoID),
			slog.String("erro", err.Error()),
		)
	}
}

// sendFailure envia e-mail de falha — best-effort, falha apenas logada.
func (n *notifier) sendFailure(ctx context.Context, toEmail, videoID, errMsg string) {
	subject := "Falha no processamento do seu vídeo"
	body := fmt.Sprintf(
		"Olá!\n\nOcorreu um erro ao processar seu vídeo.\n\n"+
			"ID do vídeo: %s\nMotivo: %s\n\n"+
			"Entre em contato se precisar de ajuda.",
		videoID, errMsg,
	)
	if err := n.send(ctx, toEmail, subject, body); err != nil {
		slog.Error("falha ao enviar e-mail de erro (best-effort)",
			slog.String("video_id", videoID),
			slog.String("erro", err.Error()),
		)
	}
}

func (n *notifier) send(ctx context.Context, toEmail, subject, body string) error {
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
