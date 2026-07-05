# scripts/

## localstack-init.sh

Executado automaticamente pelo LocalStack no boot (montado em `/etc/localstack/init/ready.d/`). Não é necessário rodar manualmente — o `docker-compose.dev.yml` faz a montagem.

**O que cria:**

| Recurso | Nome |
|---------|------|
| S3 bucket | `framecast-videos-raw` (entrada: vídeos brutos) |
| S3 bucket | `framecast-videos-output` (saída: ZIPs de frames) |
| SQS fila | `framecast-processing` |
| SQS DLQ | `framecast-processing-dlq` |
| Redrive policy | `maxReceiveCount=3` → mensagens vão para DLQ após 3 falhas |
| SES identity | sender identity para dev (simulado pelo LocalStack) |

**Uso manual** (caso necessário fora do Docker Compose):

```bash
# Requer awslocal instalado (pip install awscli-local)
# e LocalStack rodando em localhost:4566
chmod +x scripts/localstack-init.sh
./scripts/localstack-init.sh
```
