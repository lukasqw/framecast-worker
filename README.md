# framecast-worker

Consumer SQS do pipeline Framecast. Recebe eventos de vídeo enfileirados pela `framecast-api`, executa FFmpeg para extração de frames, faz streaming do ZIP para S3 e notifica o usuário via SES.

---

## Fluxo de processamento

```
SQS ReceiveMessage (long polling 20s, VisibilityTimeout=15min)
  │
  ├─ Idempotência: SELECT status FROM videos FOR UPDATE
  │    DONE / ERROR → DeleteMessage (já finalizado)
  │    PENDING / PROCESSING → adquire lease (UPDATE worker_id, attempt++)
  │
  ├─ Heartbeat goroutine: ChangeMessageVisibility +15min a cada 5min
  │
  ├─ Download: S3.GetObject(raw) → /tmp/<video_id>/input.mp4
  │
  ├─ FFmpeg: extrai 1 frame/segundo → /tmp/<video_id>/frames/frame_%04d.png
  │    Falha (codec/corrompido/timeout) → status=ERROR + SES + DeleteMessage
  │
  ├─ ZIP streaming: io.Pipe → zip.Writer → s3manager.Upload (sem disco)
  │
  ├─ TX: UPDATE videos SET status='DONE', s3_key_output, frame_count
  │
  ├─ SES: e-mail de conclusão (best-effort)
  │
  └─ DeleteMessage (ACK)
      defer os.RemoveAll(/tmp/<video_id>)
```

---

## Estrutura

```
framecast-worker/
├── cmd/worker/main.go               # wire-up + graceful shutdown (SIGTERM)
├── internal/
│   ├── config/config.go             # env vars fail-fast
│   ├── consumer/consumer.go         # loop SQS + semáforo de concorrência
│   ├── processor/
│   │   ├── processor.go             # orquestrador principal
│   │   ├── lease.go                 # idempotência + FOR UPDATE
│   │   ├── heartbeat.go             # ChangeMessageVisibility goroutine
│   │   ├── downloader.go            # S3.GetObject → disco
│   │   ├── ffmpeg.go                # exec.CommandContext + contagem de frames
│   │   ├── zip_upload.go            # io.Pipe + zip.Writer + s3manager
│   │   └── notifier.go              # SES sucesso + falha (best-effort)
│   └── infra/
│       ├── awsclient/               # S3 + SQS + SESv2 (UsePathStyle LocalStack)
│       ├── database/                # GORM connect (sem AutoMigrate)
│       └── observability/           # slog JSON + OTel OTLP gRPC
├── scripts/localstack-init.sh       # buckets, DLQ, fila, SES identities
├── Dockerfile                       # multi-stage: golang:1.25-alpine + ffmpeg
├── docker-compose.dev.yml
└── .github/workflows/
    ├── ci.yml                       # lint + test + build + docker
    └── deploy.yml                   # OIDC → ECR → EKS
```

---

## Tratamento de falhas

| Falha | Retentável | Ação |
|---|---|---|
| Download S3 | Sim | Visibility expira → SQS reenvia (até DLQ em 3x) |
| FFmpeg erro/timeout | **Não** | `status=ERROR` + SES falha + `DeleteMessage` |
| ZIP/upload falha | Sim | SQS reenvia, refaz do zero |
| `markDone` DB falha | Sim | SQS reenvia; `FOR UPDATE` protege reprocessamento |
| Worker crasha | Sim | Heartbeat para → visibility expira → SQS reenvia |
| SES falha | — | Best-effort, log, não bloqueia ACK |

---

## Variáveis de ambiente

```env
DATABASE_URL=postgres://framecast:framecast@localhost:5432/framecast_db?sslmode=disable

AWS_ENDPOINT_URL=http://localhost:4566   # vazio = AWS real
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test

S3_BUCKET_RAW=framecast-videos-raw
S3_BUCKET_OUTPUT=framecast-videos-output

SQS_QUEUE_URL=http://localhost:4566/000000000000/framecast-processing

SES_FROM_EMAIL=noreply@framecast.local
SES_RECIPIENT_OVERRIDE=dev@framecast.local   # dev: redireciona todos os e-mails

WORKER_CONCURRENCY=1          # vídeos por pod
FFMPEG_TIMEOUT_MINUTES=30

OTEL_EXPORTER_OTLP_ENDPOINT=  # vazio = OTel desabilitado (dev)
APP_ENV=dev
APP_VERSION=local
DD_SERVICE=framecast-worker
```

---

## Dev local

> **Pré-requisito:** rodar a `framecast-api` ao menos uma vez para criar o schema via GORM AutoMigrate.

```bash
cd framecast-worker
cp .env.example .env

# Postgres + LocalStack
docker compose -f docker-compose.dev.yml up postgres localstack -d

# Worker em modo dev (com go run)
go run ./cmd/worker
```

Subir tudo com Docker:

```bash
docker compose -f docker-compose.dev.yml up
```

---

## Métricas OTel

| Métrica | Tipo | Atributos |
|---|---|---|
| `framecast.video.processed.total` | counter | `status: done\|error` |
| `framecast.video.processing.duration` | histogram (s) | `status: done\|error` |
| `framecast.video.frame_count` | histogram | — |
| `framecast.ffmpeg.duration` | histogram (s) | — |

---

## Notas de design

- **Schema:** o worker **não executa AutoMigrate** — a `framecast-api` é dona do schema.
- **Concorrência:** `WORKER_CONCURRENCY=1` por pod; paralelismo vem de N pods via KEDA.
- **Idempotência:** `SELECT FOR UPDATE` garante que dois pods não processem o mesmo vídeo.
- **ZIP streaming:** `io.Pipe` conecta `zip.Writer` ao `s3manager.Upload` — sem materializar o ZIP em disco, independente do tamanho do arquivo.
- **workerID:** `os.Hostname()` — no K8s, é o nome do pod, facilitando correlação de logs.
