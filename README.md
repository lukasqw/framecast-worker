# framecast-worker

Consumer SQS do pipeline Framecast. Recebe eventos de vídeo enfileirados pela `framecast-api`, executa FFmpeg para extração de frames (`FFMPEG_FPS`, padrão 1 frame/segundo), faz streaming do ZIP para S3 e notifica o usuário via e-mail.

---

## Fluxo de processamento

```
SQS ReceiveMessage (long poll 20s, VisibilityTimeout=15min, batch até 10 mensagens)
  │
  ├─ JSON parse → Message{video_id, user_id, s3_key, bucket, output_bucket}
  │   inválido → DeleteMessage + skip
  │
  ├─ Semáforo: adquire slot (máx WORKER_CONCURRENCY=3 simultâneos por pod)
  │
  ├─ Idempotência: SELECT ... FOR UPDATE (TX)
  │     DONE / ERROR     → DeleteMessage + return  (dedup silencioso)
  │     lease vivo       → return error (SQS reentrega após visibility)
  │     PENDING / stale  → UPDATE worker_id, attempt++, last_heartbeat_at
  │
  ├─ workDir = os.MkdirTemp("", "framecast-*")
  │   defer os.RemoveAll(workDir)
  │
  ├─ Heartbeat goroutine (a cada 1min):
  │     ChangeMessageVisibility +15min  +  UPDATE last_heartbeat_at
  │
  ├─ Download: S3.GetObject → workDir/input.mp4
  │   falha → markError + SendFailure + DeleteMessage
  │
  ├─ FFmpeg: extrai FFMPEG_FPS frames/segundo (padrão 1) → workDir/frames/frame_%04d.png
  │   falha (codec/corrompido/timeout) → markError + SendFailure + DeleteMessage [não-retentável]
  │
  ├─ ZIP streaming: io.Pipe → zip.Writer (goroutine A) ↔ s3manager.Upload (inline)
  │   falha → markError + SendFailure + DeleteMessage
  │
  ├─ finalizeSuccess: UPDATE videos SET status='DONE', s3_key_output, frame_count, worker_id=NULL
  │
  ├─ Notificação de sucesso (best-effort): SendSuccess via SMTP ou SES
  │
  └─ DeleteMessage (ACK)
```

---

## Estrutura

```
framecast-worker/
├── cmd/worker/main.go               # wire-up + graceful shutdown (SIGTERM/SIGINT)
└── internal/
    ├── config/config.go             # env vars com validação fail-fast
    ├── consumer/consumer.go         # loop SQS + semáforo de concorrência
    ├── processor/
    │   ├── processor.go             # orquestrador principal (Process)
    │   ├── lease.go                 # idempotência SELECT FOR UPDATE
    │   ├── heartbeat.go             # goroutine ChangeMessageVisibility + heartbeat DB
    │   ├── downloader.go            # S3.GetObject → disco
    │   ├── ffmpeg.go                # exec.CommandContext + contagem de PNGs
    │   ├── zip_upload.go            # io.Pipe + zip.Writer + s3manager.Upload
    │   ├── dlq.go                   # DLQHandler — marca ERROR + notifica + ACK
    │   └── aws_api.go               # interfaces s3API, sqsAPI (injetáveis em testes)
    └── infra/
        ├── awsclient/               # S3 + SQS + SESv2 (UsePathStyle para LocalStack)
        ├── database/                # GORM connect + OTel plugin (sem AutoMigrate)
        ├── email/                   # Notifier interface + SESNotifier + SMTPNotifier
        ├── observability/           # slog JSON + OTel OTLP gRPC (delta temporality)
        └── sqscarrier/              # W3C TraceContext extractor de MessageAttributes
```

---

## Tratamento de falhas

| Cenário | Retentável | Ação |
|---------|-----------|------|
| JSON inválido | Não | DeleteMessage + skip |
| Lease em uso (worker vivo) | Sim (SQS reentrega) | Return error; sem DeleteMessage |
| Download S3 falha | **Não** | `markError` + `SendFailure` + `DeleteMessage` |
| FFmpeg erro/timeout | **Não** | `markError` + `SendFailure` + `DeleteMessage` |
| ZIP/upload S3 falha | **Não** | `markError` + `SendFailure` + `DeleteMessage` |
| Worker crasha mid-flight | Sim | Heartbeat para → lease expira (3min) → SQS reentrega |
| 3 falhas consecutivas | Sim (DLQ) | SQS move para DLQ → `DLQHandler` marca ERROR + notifica |
| SES/SMTP falha | — | Best-effort: log apenas, não bloqueia ACK |

> **Atenção:** download, FFmpeg e ZIP são todos não-retentáveis via reentrega SQS. Retentabilidade acontece somente via crash do worker (heartbeat pára → lease expira).

---

## Variáveis de ambiente

| Variável | Obrigatória | Default | Descrição |
|----------|-------------|---------|-----------|
| `DATABASE_URL` | ✅ | — | DSN PostgreSQL |
| `S3_BUCKET_RAW` | ✅ | — | Bucket de entrada (vídeos brutos) |
| `S3_BUCKET_OUTPUT` | ✅ | — | Bucket de saída (ZIPs de frames) |
| `SQS_QUEUE_URL` | ✅ | — | URL da fila principal de processamento |
| `SES_FROM_EMAIL` | ✅ | — | Remetente SES (identidade verificada) |
| `SQS_DLQ_URL` | — | `""` | URL da DLQ — habilita DLQHandler se preenchido |
| `EMAIL_NOTIFICATIONS_ENABLED` | — | `true` | `false` desliga o envio de e-mail (usa `NoOpNotifier`) sem mudar o backend configurado |
| `NOTIFIER_BACKEND` | — | `smtp` | `smtp` ou `ses` |
| `SMTP_HOST` | — | — | Servidor SMTP (ex: `sandbox.smtp.mailtrap.io`) |
| `SMTP_PORT` | — | — | Porta SMTP (ex: `2525`) |
| `SMTP_USERNAME` | — | — | Auth SMTP; vazio → imprime no stdout (dev) |
| `SMTP_PASSWORD` | — | — | Auth SMTP |
| `SES_RECIPIENT_OVERRIDE` | — | — | Redireciona TODOS os e-mails para este endereço |
| `AWS_REGION` | — | `us-east-1` | Região AWS |
| `AWS_ENDPOINT_URL` | — | — | Override de endpoint (LocalStack) |
| `AWS_ACCESS_KEY_ID` | — | — | Credencial AWS / `test` no LocalStack |
| `AWS_SECRET_ACCESS_KEY` | — | — | Credencial AWS / `test` no LocalStack |
| `AWS_SESSION_TOKEN` | — | — | Token de sessão temporária (prod/CI) |
| `WORKER_CONCURRENCY` | — | `3` | Slots paralelos por pod |
| `FFMPEG_TIMEOUT_MINUTES` | — | `30` | Timeout do processo FFmpeg |
| `FFMPEG_FPS` | — | `1` | Frames extraídos por segundo de vídeo (`-vf fps=N`) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | `""` | Endpoint OTLP gRPC — vazio desabilita OTel |
| `APP_ENV` | — | `""` | Ambiente (`dev`, `staging`, `production`) |
| `APP_VERSION` | — | `""` | Versão propagada nos spans |
| `DD_SERVICE` | — | `framecast-worker` | Nome do serviço no Datadog |

Copie `.env.example` como ponto de partida.

---

## Dev local

> **Pré-requisito:** a `framecast-api` deve rodar ao menos uma vez para criar o schema via GORM AutoMigrate — o worker não executa AutoMigrate.

```bash
cd framecast-worker
cp .env.example .env

# Subir Postgres + LocalStack
docker compose -f docker-compose.dev.yml up postgres localstack -d

# Rodar o worker
go run ./cmd/worker
```

Subir tudo com Docker (worker + dependências):

```bash
docker compose -f docker-compose.dev.yml up
```

---

## E-mail — configuração por ambiente

### Dev local (padrão)

`SMTP_USERNAME` e `SMTP_PASSWORD` vazios → e-mails são impressos no stdout. Nenhuma conexão SMTP é feita.

### SMTP real (Mailtrap, etc.)

```env
NOTIFIER_BACKEND=smtp
SMTP_HOST=sandbox.smtp.mailtrap.io
SMTP_PORT=2525
SMTP_USERNAME=<usuario>
SMTP_PASSWORD=<senha>
```

### SES com LocalStack

O `docker-compose.dev.yml` já inicializa o LocalStack com SES simulado. Use:

```env
NOTIFIER_BACKEND=ses
AWS_ENDPOINT_URL=http://localhost:4566
SES_FROM_EMAIL=noreply@framecast.local
SES_RECIPIENT_OVERRIDE=dev@framecast.local
```

### SES na AWS — sandbox (AWS Academy)

> **AWS Academy mantém SES em sandbox permanentemente.** Remetente e destinatário precisam ser endereços verificados.

```bash
# Verificar endereço de e-mail
aws ses verify-email-identity --email-address seuemail@gmail.com --region us-east-1
```

```env
NOTIFIER_BACKEND=ses
AWS_ENDPOINT_URL=
SES_FROM_EMAIL=seuemail@gmail.com
SES_RECIPIENT_OVERRIDE=seuemail@gmail.com   # redireciona todos os e-mails para você
```

Com `SES_RECIPIENT_OVERRIDE` configurado, todos os e-mails chegam no mesmo endereço, independente do usuário que fez o upload — contorna a restrição de destinatário não-verificado do sandbox.

| Evento | Assunto do e-mail |
|--------|------------------|
| FFmpeg conclui com sucesso | "Seu vídeo está pronto para download" |
| FFmpeg falha (codec/timeout) | "Falha no processamento do seu vídeo" |
| Mensagem vai para DLQ (3× tentativas) | "Falha no processamento do seu vídeo" |

Layout HTML moderno (card escuro, com fallback texto-plano para clientes sem suporte a
HTML) — templates em `internal/infra/email/template.go`. O e-mail de sucesso inclui um
botão com a URL de download direto do ZIP (S3 presigned, TTL 7 dias, best-effort: se a
geração da URL falhar, o e-mail sai sem o botão em vez de bloquear a notificação).
`EMAIL_NOTIFICATIONS_ENABLED=false` desliga o envio inteiro (`NoOpNotifier`).

---

## Métricas OTel

| Métrica | Tipo | Atributos | Descrição |
|---------|------|-----------|-----------|
| `framecast.video.processed.total` | Counter | `status: done\|error` | Vídeos finalizados |
| `framecast.video.processing.duration` | Histogram (s) | `status: done\|error` | Tempo total de processamento |
| `framecast.video.frame_count` | Histogram | — | Frames extraídos por vídeo |
| `framecast.ffmpeg.duration` | Histogram (s) | — | Duração do processo FFmpeg |
| `framecast.worker.sqs.messages.received` | Counter | — | Mensagens recebidas do SQS |

Métricas exportadas via OTLP gRPC com **delta temporality** (requisito do Datadog Agent).  
OTel desabilitado quando `OTEL_EXPORTER_OTLP_ENDPOINT` está vazio.

---

## Testes

```bash
# Testes unitários (sem infra)
go test -v -race ./...

# Cobertura
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

Testes usam `go-sqlmock` para GORM e interfaces injetáveis (`s3API`, `sqsAPI`, `sesAPI`) — sem dependência de infra real.

---

## CI/CD

| Workflow | Trigger | O que faz |
|----------|---------|-----------|
| `ci.yml` | Push/PR em `develop` ou `main` | lint → tests (gate 60%) + `govulncheck` → build |
| `release.yml` | Push em `develop` | Cria/atualiza branch `release/vX.Y.Z` + draft PR para main |
| `deploy.yml` | PR de `release/*` mergeado em `main` | Build + push GHCR → deploy EKS (dev → aprovação → prod) → GitHub Release |
| `rollback.yml` | `workflow_dispatch` (versão + ambiente) | Reverte imagem no EKS para tag especificada |

**Imagens:** publicadas no **GitHub Container Registry** (`ghcr.io/<owner>/framecast-worker:<sha>`).  
**Credenciais:** `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` + `AWS_SESSION_TOKEN` (estáticas, compatíveis com AWS Academy LabRole).  
**Deploy:** lê outputs do Terraform (S3 remote state) para montar ConfigMap + Secret antes de `kubectl set image`.

---

## Kubernetes (EKS)

| Recurso | Configuração |
|---------|-------------|
| `Deployment` | 1 réplica inicial; imagem via `kubectl set image` |
| `Service` | Nenhum (sem endpoint HTTP) |
| `ScaledObject` (KEDA) | min **2** / max **10** — trigger SQS, 1 pod a cada 3 mensagens |
| KEDA polling | 10s · cooldown 60s · `scaleOnInFlight=true` |
| Resources | requests: `250m/256Mi` · limits: `1000m/1Gi` |
| OTel endpoint | `http://$(DD_AGENT_HOST):4317` (Downward API → nó do DaemonSet Datadog) |

`minReplicaCount=2` garante dois pods prontos para evitar cold start durante demos.

---

## Notas de design

- **Schema:** o worker não executa AutoMigrate — a `framecast-api` é dona do schema.
- **workerID:** UUID gerado uma vez no boot (não `os.Hostname()`). Identificação do pod em logs e no campo `worker_id` da tabela `videos`.
- **ZIP streaming:** `io.Pipe` conecta `zip.Writer` ao `s3manager.Upload` — sem materializar o ZIP em disco, independente do tamanho do arquivo.
- **DLQ consumer:** ativado somente quando `SQS_DLQ_URL` está preenchido; processa com `concurrency=1`.
- **Trace propagation:** OTel W3C TraceContext extraído dos `MessageAttributes` do SQS (`traceparent`/`tracestate`), ligando o span do consumer ao span do produtor na `framecast-api`.

Ver [docs/architecture.md](docs/architecture.md) para diagrama detalhado e decisões técnicas.
