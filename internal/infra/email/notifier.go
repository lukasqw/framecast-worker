package email

import "context"

// Notifier sends email notifications for video processing events.
// Two backends implement this: SESNotifier (AWS SES) and SMTPNotifier (SMTP/Mailtrap).
// Selected at startup via NOTIFIER_BACKEND config ("ses" | "smtp").
type Notifier interface {
	// downloadURL é opcional (string vazia): quando presente, o e-mail de sucesso
	// inclui um botão de download direto para o ZIP de frames (presigned S3 URL).
	SendSuccess(ctx context.Context, toEmail, videoID, filename, downloadURL string)
	SendFailure(ctx context.Context, toEmail, videoID, errMsg string)
}

// NoOpNotifier silently discards all notifications.
type NoOpNotifier struct{}

func (NoOpNotifier) SendSuccess(context.Context, string, string, string, string) {}
func (NoOpNotifier) SendFailure(context.Context, string, string, string)         {}
