// Lógica de notificação movida para internal/infra/email.
// SESNotifier e SMTPNotifier implementam email.Notifier.
// Wire-up em cmd/worker/main.go via NOTIFIER_BACKEND.
package processor
