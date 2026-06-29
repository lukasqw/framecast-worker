package email

import "context"

// Notifier sends email notifications for video processing events.
// Two backends implement this: SESNotifier (AWS SES) and SMTPNotifier (SMTP/Mailtrap).
// Selected at startup via NOTIFIER_BACKEND config ("ses" | "smtp").
type Notifier interface {
	SendSuccess(ctx context.Context, toEmail, videoID, filename string)
	SendFailure(ctx context.Context, toEmail, videoID, errMsg string)
}

// NoOpNotifier silently discards all notifications.
type NoOpNotifier struct{}

func (NoOpNotifier) SendSuccess(context.Context, string, string, string) {}
func (NoOpNotifier) SendFailure(context.Context, string, string, string) {}
