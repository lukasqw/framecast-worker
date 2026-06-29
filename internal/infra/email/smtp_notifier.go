package email

import (
	"context"
	"fmt"
	"log/slog"
	"net/smtp"
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

func (s *SMTPNotifier) SendSuccess(ctx context.Context, toEmail, videoID, filename string) {
	subject := "Seu vídeo está pronto para download"
	body := fmt.Sprintf(
		"Olá!\n\nSeu vídeo %q foi processado com sucesso.\n"+
			"Acesse a plataforma para baixar os frames extraídos.\n\n"+
			"ID do vídeo: %s",
		filename, videoID,
	)
	if err := s.send(toEmail, subject, body); err != nil {
		slog.ErrorContext(ctx, "falha ao enviar e-mail de sucesso via SMTP (best-effort)",
			slog.String("video_id", videoID),
			slog.String("erro", err.Error()),
		)
	}
}

func (s *SMTPNotifier) SendFailure(ctx context.Context, toEmail, videoID, errMsg string) {
	subject := "Falha no processamento do seu vídeo"
	body := fmt.Sprintf(
		"Olá!\n\nOcorreu um erro ao processar seu vídeo.\n\n"+
			"ID do vídeo: %s\nMotivo: %s\n\n"+
			"Entre em contato se precisar de ajuda.",
		videoID, errMsg,
	)
	if err := s.send(toEmail, subject, body); err != nil {
		slog.ErrorContext(ctx, "falha ao enviar e-mail de erro via SMTP (best-effort)",
			slog.String("video_id", videoID),
			slog.String("erro", err.Error()),
		)
	}
}

func (s *SMTPNotifier) send(to, subject, body string) error {
	// Dev mode: sem credenciais imprime no stdout
	if s.username == "" || s.password == "" {
		fmt.Printf("[SMTP-DEV] To: %s | Subject: %s\n%s\n", to, subject, body)
		return nil
	}
	addr := s.host + ":" + s.port
	auth := smtp.PlainAuth("", s.username, s.password, s.host)
	msg := []byte(fmt.Sprintf(
		"From: %s\r\nTo: %s\r\nSubject: %s\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s\r\n",
		s.from, to, subject, body,
	))
	return smtp.SendMail(addr, auth, s.from, []string{to}, msg)
}
