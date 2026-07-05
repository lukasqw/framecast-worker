# framecast-worker — contexto para Claude

## Identidade do repo

Go 1.25 · Consumer SQS · FFmpeg · GORM (sem AutoMigrate) · OTel delta · SES/SMTP

Módulo Go: `github.com/lukasqw/framecast-worker`  
Entry point: `cmd/worker/main.go`

## Estrutura de pacotes

```
internal/config/        → env vars fail-fast (Load())
internal/consumer/      → loop SQS + semáforo de concorrência (concurrency slots)
internal/processor/     → orquestrador + lease + heartbeat + downloader + ffmpeg + zip + dlq
internal/infra/
    awsclient/          → S3 + SQS + SESv2 (UsePathStyle LocalStack)
    database/           → GORM connect + OTel plugin (sem AutoMigrate)
    email/              → Notifier interface, SESNotifier, SMTPNotifier, NoOpNotifier
    observability/      → slog JSON + OTel OTLP gRPC (delta) + helpers SpanWorker/SpanConsumer
    sqscarrier/         → extrai W3C TraceContext de MessageAttributes SQS
```

## Fluxo central

```
Consumer.Run → semáforo → goroutine → processor.Process:
  acquireLease (SELECT FOR UPDATE) → heartbeat (1min) → download S3 →
  FFmpeg (fps=FFMPEG_FPS, padrão 1, PNGs) → zipAndUpload (io.Pipe streaming) →
  finalizeSuccess (UPDATE DONE) → SendSuccess (best-effort) → DeleteMessage
```

## Idempotência

`SELECT ... FOR UPDATE` na tabela `videos`. Lease via `worker_id` (UUID, não hostname) + `last_heartbeat_at`. Lease TTL = 3min = 3× heartbeatInterval (1min). Worker morto → heartbeat para → outro pod reassume após 3min.

## Erros — TODOS são não-retentáveis exceto crash

Download S3, FFmpeg, ZIP → `markError + SendFailure + DeleteMessage` (sem reentrada SQS).  
Crash do worker → SQS reentrega após visibility expirar → até 3× → DLQ.  
DLQ consumer (ativado por `SQS_DLQ_URL`) → `markError + notifica + DeleteMessage`.

## Notificação de e-mail

`EMAIL_NOTIFICATIONS_ENABLED` (padrão `true`) liga/desliga o envio sem mexer em qual
backend está configurado — desligar e religar depois não exige reconfigurar
`NOTIFIER_BACKEND`. Quando `false`, usa `email.NoOpNotifier` (descarta silenciosamente).  
Backend selecionado por `NOTIFIER_BACKEND`: `ses` (SESv2) ou `smtp` (padrão).  
SMTP com credenciais vazias → imprime no stdout (modo dev, sem conexão real).  
`SES_RECIPIENT_OVERRIDE` redireciona todos os e-mails para um endereço fixo (dev/sandbox SES Academy).  
E-mail de sucesso inclui a URL de download do ZIP (S3 presigned, TTL 7 dias — best-effort, gerada em `internal/processor/processor.go`).

## Convenções de código

- Interfaces injetáveis em todos os pacotes de infra (`s3API`, `sqsAPI`, `sesAPI`) → testes unitários via go-sqlmock e interfaces fake
- Sem comentários óbvios; apenas WHY não-óbvio
- Logs em português via `log/slog` JSON; `LoggerFromContext(ctx)` para correlação trace
- Métricas com delta temporality (requisito Datadog Agent)

## CI/CD (realidade, não docs antigos)

- Imagens no **GHCR** — não ECR
- Credenciais **estáticas** (`AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY` + `AWS_SESSION_TOKEN`) — não OIDC
- Coverage gate real na CI: **60%** (não 85% — variável `COVERAGE_GATE` existe mas não é consumida pela CI)
- Workflows: `ci.yml`, `release.yml`, `deploy.yml` (dev → aprovação → prod), `rollback.yml`

## KEDA

`minReplicaCount=2` (não 1 como diz o CLAUDE.md raiz), `maxReplicaCount=10`, `queueLength=3`, `pollingInterval=10s`, `cooldownPeriod=60s`.

## Dev local

```bash
docker compose -f docker-compose.dev.yml up postgres localstack -d
cp .env.example .env
go run ./cmd/worker
# Requer: framecast-api rodando ao menos uma vez (AutoMigrate cria schema)
```

## Documentação adicional

- `docs/architecture.md` — diagrama completo, lease, streaming ZIP, máquina de estados, decisões técnicas
