# Arquitetura — framecast-worker

## Posição no sistema

```
framecast-api (outbox dispatcher)
    │
    │  SQS SendMessage
    │  Payload JSON: {video_id, user_id, s3_key, bucket, output_bucket}
    │  MessageAttributes: W3C traceparent/tracestate (OTel propagation)
    ▼
┌─────────────────────────────────────────────────────────┐
│                    framecast-worker                      │
│                                                         │
│  Consumer loop                                          │
│    ReceiveMessage (long poll 20s, batch 10, vis 900s)   │
│         │                                               │
│    sem ── goroutine (max WORKER_CONCURRENCY=3)          │
│         │                                               │
│    ┌────▼──────────────────────────────────────────┐   │
│    │  processor.Process()                           │   │
│    │    acquireLease (Postgres FOR UPDATE)          │   │
│    │    startHeartbeat goroutine (1min interval)    │   │
│    │    downloadVideo (S3 → workDir/input.mp4)      │   │
│    │    runFFmpeg → workDir/frames/frame_%04d.png   │   │
│    │    zipAndUpload (io.Pipe streaming → S3)       │   │
│    │    finalizeSuccess (UPDATE status=DONE)        │   │
│    │    notif.SendSuccess (best-effort)             │   │
│    │    DeleteMessage                               │   │
│    └───────────────────────────────────────────────┘   │
│                                                         │
│  DLQHandler (opcional, concurrency=1)                   │
│    ReceiveMessage(DLQ) → markError + notify + ACK       │
└─────────────────────────────────────────────────────────┘
         │              │              │
    ┌────▼────┐   ┌─────▼────┐  ┌────▼────┐
    │ S3 raw  │   │ Postgres │  │ S3 out  │
    │ (input) │   │ (status) │  │ (ZIP)   │
    └─────────┘   └──────────┘  └─────────┘
                        │
                 ┌──────▼──────┐
                 │  SES/SMTP   │
                 │  (e-mail)   │
                 └─────────────┘
```

---

## Modelo de concorrência

```
Consumer.Run(ctx)
    │
    ├─ receive() → []sqs.Message (batch até 10)
    │
    └─ for msg in messages:
           sem <- struct{}{}     ← bloqueia se WORKER_CONCURRENCY slots ocupados
           wg.Add(1)
           go func():
               defer wg.Done()
               defer <-sem       ← libera slot ao terminar
               processor.Process(ctx, msg, receiptHandle)

DLQ consumer: instância separada de Consumer com concurrency=1
Heartbeat: 1 goroutine por mensagem em processamento (stopHeartbeat = ctx.CancelFunc)

Graceful shutdown:
    signal (SIGTERM/SIGINT) → cancel root ctx
    → receive() retorna
    → wg.Wait() drena goroutines em-voo
    → OTel flush + DB close
```

**KEDA no EKS:** escala pods baseado em profundidade da fila SQS (`queueLength=3` = 1 pod a cada 3 mensagens em fila + in-flight). `minReplicaCount=2` garante dois pods sempre prontos.

---

## Idempotência e lease

O campo `worker_id` e `last_heartbeat_at` na tabela `videos` implementam um lease distribuído:

```
┌──────────────────────────────────────────────────────────────┐
│ acquireLease(ctx, db, videoID, workerID)                     │
│                                                              │
│  BEGIN                                                       │
│  SELECT id, status, worker_id, attempt,                     │
│         user_id, original_name, s3_key_raw,                 │
│         last_heartbeat_at                                   │
│    FROM videos WHERE id = $1 FOR UPDATE                     │
│                                                              │
│  switch status:                                             │
│    DONE      → errAlreadyDone   (ACK silencioso)            │
│    ERROR     → errAlreadyError  (ACK silencioso)            │
│    PROCESSING + last_heartbeat_at < NOW() - 3min            │
│              → acquire (worker morreu, reassume)            │
│    PROCESSING + heartbeat fresco                            │
│              → errLeaseHeld (SQS reentrega)                 │
│    PENDING   → acquire                                      │
│                                                             │
│  UPDATE videos                                              │
│     SET worker_id = $workerID,                              │
│         attempt   = attempt + 1,                            │
│         last_heartbeat_at = NOW(),                          │
│         updated_at        = NOW()                           │
│   WHERE id = $1                                             │
│  COMMIT                                                     │
└──────────────────────────────────────────────────────────────┘
```

**Lease TTL:** `leaseTTL = 3 × heartbeatInterval = 3 × 1min = 3 min`

Se o worker craschar, o heartbeat para, `last_heartbeat_at` fica estático. Após 3 minutos, outro worker reassume o processamento (SQS reentrega após visibility expirar).

---

## Campos da tabela `videos` usados pelo worker

| Coluna | Tipo | Lido/Escrito | Uso |
|--------|------|-------------|-----|
| `id` | uuid | Lido | Identificação do vídeo |
| `status` | varchar | Lido + Escrito | Máquina de estados |
| `worker_id` | varchar | Lido + Escrito | Lease — UUID do worker |
| `attempt` | int | Lido + Escrito | Contador de tentativas |
| `last_heartbeat_at` | timestamptz | Lido + Escrito | Detecção de worker morto |
| `user_id` | varchar | Lido | Para buscar e-mail do usuário |
| `original_name` | varchar | Lido | Nome no e-mail de notificação |
| `s3_key_raw` | varchar | Lido | Chave S3 do vídeo original |
| `s3_key_output` | varchar | Escrito | Chave S3 do ZIP gerado |
| `error_message` | text | Escrito | Mensagem de erro (status=ERROR) |
| `updated_at` | timestamptz | Escrito | |

> O worker não executa AutoMigrate. O schema é de responsabilidade exclusiva da `framecast-api`.

---

## ZIP streaming sem disco

```
framesDir  ←  workDir/frames/frame_0001.png, frame_0002.png, ...
                                │
             ┌──────────────────┼──────────────────────────┐
             │  goroutine A     │                           │
             │                  ▼                           │
             │  pw ──► zip.Writer                          │
             │           │                                  │
             │           └─ zw.Create("frame_0001.png")     │
             │              io.Copy(ze, f)                  │
             │              ... todos os PNGs ...           │
             │              zw.Close()                      │
             │              pw.CloseWithError(err)          │
             └──────────────────────────────────────────────┘
                         io.Pipe
             pr ──────────────────────────────────────────►
                                                           │
                                               s3manager.Upload
                                               Bucket: output
                                               Key: <videoID>.zip
                                               Body: pr
```

`io.Pipe` garante que bytes do ZIP fluem diretamente para o S3 sem buffer em disco. O S3 Manager usa upload multipart internamente quando o body é um `io.Reader`.

---

## Propagação de trace OTel

```
framecast-api                          framecast-worker
    │                                        │
    │  outbox dispatcher                     │
    │  SQS SendMessage                       │
    │    MessageAttributes:                  │
    │      traceparent = "00-<traceID>-..."  │
    │                                        │
    │                             consumer.Run()
    │                               sqscarrier.ExtractCarrier(msg.MessageAttributes)
    │                               propagation.Extract(ctx, carrier)
    │                               SpanConsumer(ctx, "sqs.receive")
    │                                 └─ SpanKind=Consumer
    │                                    links to producer trace
    │                                 processor.Process(ctx, ...)
    │                                   SpanWorker(ctx, "ffmpeg")
    │                                   SpanWorker(ctx, "zip_upload")
    │                                   ...
```

O span do consumer aparece ligado ao trace da `framecast-api` no Datadog, permitindo rastrear toda a jornada do upload ao processamento em uma única trace distribuída.

---

## Máquina de estados do vídeo

```
           framecast-api                    framecast-worker
                │                                  │
           PENDING (upload init)                   │
                │                                  │
           PROCESSING (upload complete)            │
                │                                  │
                ├──── SQS message ─────────────────┤
                │                              acquireLease
                │                                  │
                │                    ┌─────────────┴──────────────┐
                │                    │                            │
                │                 success                      errLeaseHeld
                │                    │                            │
                │                 processar                   SQS reentrega
                │               ┌────┴─────┐
                │               │          │
                │            DONE        ERROR
                │         (s3_key_output) (error_message)
```

---

## Decisões técnicas

| Decisão | Alternativa descartada | Motivo |
|---------|----------------------|--------|
| `SELECT FOR UPDATE` para lease | Redis distributed lock | Evita dependência extra; Postgres já é obrigatório |
| Heartbeat a cada 1min (não 5min) | Intervalo maior | Lease TTL 3min → margem de 3 batidas antes de expirar |
| Falhas de download/FFmpeg/ZIP são não-retentáveis | Reentrega SQS | Erros de codec/corrompido nunca se resolvem sozinhos; DLQ cuida de falhas de infra |
| UUID como workerID (não `os.Hostname()`) | Hostname do pod | UUID é estável mesmo fora do K8s (dev local, testes) |
| SMTP como backend padrão | SES | AWS Academy bloqueia SES por padrão; SMTP permite dev local sem credenciais reais |
| ZIP streaming via `io.Pipe` | Materializar em disco | Sem limite de tamanho, sem IOPS extras; vídeos grandes não explodem o disco do pod |
| `minReplicaCount=2` no KEDA | `minReplicaCount=1` | Dois pods prontos eliminam cold start em demos; custo marginal |
| DLQ consumer condicional (`SQS_DLQ_URL`) | Sempre ativo | Permite deploy sem DLQ em ambientes dev/staging simples |
