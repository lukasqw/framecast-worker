package email

import (
	"context"
	"fmt"
	"log/slog"
	"net/smtp"
	"strings"
)

// SMTPNotifier envia notificações via SMTP.
// Em dev (username vazio) imprime no stdout sem tentar conexão.
// Compatível com Mailtrap (sandbox.smtp.mailtrap.io:2525) e qualquer provedor SMTP.
type SMTPNotifier struct {
	host, port, username, password, from string
}

func NewSMTPNotifier(host, port, username, password, from string) *SMTPNotifier {
	return &SMTPNotifier{host: host, port: port, username: username, password: password, from: from}
}

func (s *SMTPNotifier) SendSuccess(ctx context.Context, toEmail, videoID, filename, downloadURL string) {
	subject, html, text, err := buildSuccessEmail(filename, videoID, downloadURL)
	if err != nil {
		slog.ErrorContext(ctx, "falha ao montar e-mail de sucesso (best-effort)",
			slog.String("video_id", videoID), slog.String("erro", err.Error()))
		return
	}
	if err := s.send(toEmail, subject, html, text); err != nil {
		slog.ErrorContext(ctx, "falha ao enviar e-mail de sucesso via SMTP (best-effort)",
			slog.String("video_id", videoID),
			slog.String("erro", err.Error()),
		)
	}
}

func (s *SMTPNotifier) SendFailure(ctx context.Context, toEmail, videoID, errMsg string) {
	subject, html, text, err := buildFailureEmail(videoID, errMsg)
	if err != nil {
		slog.ErrorContext(ctx, "falha ao montar e-mail de falha (best-effort)",
			slog.String("video_id", videoID), slog.String("erro", err.Error()))
		return
	}
	if err := s.send(toEmail, subject, html, text); err != nil {
		slog.ErrorContext(ctx, "falha ao enviar e-mail de erro via SMTP (best-effort)",
			slog.String("video_id", videoID),
			slog.String("erro", err.Error()),
		)
	}
}

// multipart/alternative com as duas partes (texto + html) — clientes sem suporte
// a HTML (ou que preferem texto) renderizam a primeira parte compatível.
const smtpBoundary = "framecast-boundary-7a1c9e"

func (s *SMTPNotifier) send(to, subject, html, text string) error {
	msg := buildMIMEMessage(s.from, to, subject, html, text)

	// Dev mode: sem credenciais imprime no stdout
	if s.username == "" || s.password == "" {
		fmt.Printf("[SMTP-DEV] To: %s | Subject: %s\n%s\n", to, subject, text)
		return nil
	}
	addr := s.host + ":" + s.port
	auth := smtp.PlainAuth("", s.username, s.password, s.host)
	return smtp.SendMail(addr, auth, s.from, []string{to}, msg)
}

func buildMIMEMessage(from, to, subject, html, text string) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, "From: %s\r\n", from)
	fmt.Fprintf(&b, "To: %s\r\n", to)
	fmt.Fprintf(&b, "Subject: %s\r\n", subject)
	fmt.Fprintf(&b, "MIME-Version: 1.0\r\n")
	fmt.Fprintf(&b, "Content-Type: multipart/alternative; boundary=%s\r\n\r\n", smtpBoundary)

	fmt.Fprintf(&b, "--%s\r\n", smtpBoundary)
	fmt.Fprintf(&b, "Content-Type: text/plain; charset=UTF-8\r\n\r\n%s\r\n\r\n", text)

	fmt.Fprintf(&b, "--%s\r\n", smtpBoundary)
	fmt.Fprintf(&b, "Content-Type: text/html; charset=UTF-8\r\n\r\n%s\r\n\r\n", html)

	fmt.Fprintf(&b, "--%s--\r\n", smtpBoundary)
	return []byte(b.String())
}
