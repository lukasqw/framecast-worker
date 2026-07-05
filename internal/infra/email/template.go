package email

import (
	"bytes"
	"fmt"
	"html/template"
)

// html/template escapa filename/errMsg automaticamente — ambos vêm de input do
// usuário (nome do arquivo enviado, mensagem de erro do FFmpeg) e não devem ser
// interpolados sem escape num e-mail HTML.
var successTmpl = template.Must(template.New("success").Parse(`<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0f1115;font-family:-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#0f1115;padding:32px 16px;">
    <tr><td align="center">
      <table role="presentation" width="480" cellpadding="0" cellspacing="0" style="max-width:480px;width:100%;background:#181b22;border-radius:16px;overflow:hidden;">
        <tr>
          <td style="background:linear-gradient(135deg,#6C5CE7,#4834d4);padding:28px 32px;">
            <span style="color:#ffffff;font-size:20px;font-weight:700;letter-spacing:-.02em;">Framecast</span>
          </td>
        </tr>
        <tr>
          <td style="padding:32px 32px 8px;">
            <div style="width:44px;height:44px;background:#1f7a4d;border-radius:50%;color:#ffffff;font-size:22px;line-height:44px;text-align:center;">&#10003;</div>
            <h1 style="color:#ffffff;font-size:20px;margin:20px 0 8px;">Vídeo processado com sucesso!</h1>
            <p style="color:#a0a5b1;font-size:14px;line-height:1.6;margin:0 0 8px;">
              Os frames de <strong style="color:#e4e6eb;">{{.Filename}}</strong> já foram extraídos e estão prontos para download.
            </p>
          </td>
        </tr>
        {{if .DownloadURL}}
        <tr>
          <td align="center" style="padding:8px 32px 28px;">
            <a href="{{.DownloadURL}}" style="display:inline-block;background:#6C5CE7;color:#ffffff;text-decoration:none;font-weight:600;font-size:15px;padding:14px 32px;border-radius:8px;">Baixar frames (.zip)</a>
            <p style="color:#5c616e;font-size:11px;margin:14px 0 0;">O link expira em 7 dias, junto com a retenção do arquivo.</p>
          </td>
        </tr>
        {{else}}
        <tr>
          <td style="padding:0 32px 28px;">
            <p style="color:#a0a5b1;font-size:14px;line-height:1.6;margin:0;">Acesse a plataforma para baixar os frames extraídos.</p>
          </td>
        </tr>
        {{end}}
        <tr>
          <td style="padding:0 32px 28px;">
            <div style="background:#20242e;border-radius:10px;padding:14px 18px;">
              <span style="color:#767b87;font-size:11px;">ID DO VÍDEO</span><br>
              <span style="color:#c7cad1;font-size:13px;font-family:monospace;">{{.VideoID}}</span>
            </div>
          </td>
        </tr>
        <tr>
          <td style="padding:20px 32px;border-top:1px solid #23262f;">
            <p style="color:#5c616e;font-size:12px;margin:0;">Framecast — pipeline de extração de frames de vídeo</p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>`))

var failureTmpl = template.Must(template.New("failure").Parse(`<!DOCTYPE html>
<html lang="pt-BR">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#0f1115;font-family:-apple-system,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#0f1115;padding:32px 16px;">
    <tr><td align="center">
      <table role="presentation" width="480" cellpadding="0" cellspacing="0" style="max-width:480px;width:100%;background:#181b22;border-radius:16px;overflow:hidden;">
        <tr>
          <td style="background:linear-gradient(135deg,#6C5CE7,#4834d4);padding:28px 32px;">
            <span style="color:#ffffff;font-size:20px;font-weight:700;letter-spacing:-.02em;">Framecast</span>
          </td>
        </tr>
        <tr>
          <td style="padding:32px 32px 8px;">
            <div style="width:44px;height:44px;background:#a13030;border-radius:50%;color:#ffffff;font-size:22px;line-height:44px;text-align:center;">&#33;</div>
            <h1 style="color:#ffffff;font-size:20px;margin:20px 0 8px;">Falha no processamento</h1>
            <p style="color:#a0a5b1;font-size:14px;line-height:1.6;margin:0 0 24px;">
              Não foi possível concluir o processamento do seu vídeo. Entre em contato se precisar de ajuda.
            </p>
          </td>
        </tr>
        <tr>
          <td style="padding:0 32px 28px;">
            <div style="background:#20242e;border-radius:10px;padding:14px 18px;margin-bottom:12px;">
              <span style="color:#767b87;font-size:11px;">ID DO VÍDEO</span><br>
              <span style="color:#c7cad1;font-size:13px;font-family:monospace;">{{.VideoID}}</span>
            </div>
            <div style="background:#2a1c1c;border-left:3px solid #a13030;border-radius:6px;padding:12px 16px;">
              <span style="color:#e8a0a0;font-size:13px;line-height:1.5;">{{.ErrMsg}}</span>
            </div>
          </td>
        </tr>
        <tr>
          <td style="padding:20px 32px;border-top:1px solid #23262f;">
            <p style="color:#5c616e;font-size:12px;margin:0;">Framecast — pipeline de extração de frames de vídeo</p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>`))

type successData struct {
	Filename    string
	VideoID     string
	DownloadURL string
}

type failureData struct {
	VideoID string
	ErrMsg  string
}

// buildSuccessEmail monta o assunto, corpo HTML (layout moderno, com botão de
// download quando downloadURL não está vazia) e corpo texto-plano (fallback para
// clientes de e-mail sem suporte a HTML) da notificação de sucesso.
func buildSuccessEmail(filename, videoID, downloadURL string) (subject, html, text string, err error) {
	var buf bytes.Buffer
	if err := successTmpl.Execute(&buf, successData{Filename: filename, VideoID: videoID, DownloadURL: downloadURL}); err != nil {
		return "", "", "", fmt.Errorf("renderizar template de sucesso: %w", err)
	}

	linkLine := "Acesse a plataforma para baixar os frames extraídos."
	if downloadURL != "" {
		linkLine = "Baixe direto por aqui (link válido por 7 dias): " + downloadURL
	}
	text = fmt.Sprintf(
		"Olá!\n\nSeu vídeo %q foi processado com sucesso.\n%s\n\nID do vídeo: %s",
		filename, linkLine, videoID,
	)
	return "Seu vídeo está pronto para download", buf.String(), text, nil
}

// buildFailureEmail monta o assunto, corpo HTML e corpo texto-plano da notificação
// de falha no processamento.
func buildFailureEmail(videoID, errMsg string) (subject, html, text string, err error) {
	var buf bytes.Buffer
	if err := failureTmpl.Execute(&buf, failureData{VideoID: videoID, ErrMsg: errMsg}); err != nil {
		return "", "", "", fmt.Errorf("renderizar template de falha: %w", err)
	}

	text = fmt.Sprintf(
		"Olá!\n\nOcorreu um erro ao processar seu vídeo.\n\nID do vídeo: %s\nMotivo: %s\n\nEntre em contato se precisar de ajuda.",
		videoID, errMsg,
	)
	return "Falha no processamento do seu vídeo", buf.String(), text, nil
}
