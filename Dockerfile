FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /framecast-worker ./cmd/worker

# ─────────────────────────────────────────────────────────────────────────────
FROM alpine:3.21

# ffmpeg é o único binário extra necessário em runtime
RUN apk add --no-cache ffmpeg ca-certificates tzdata

WORKDIR /app
COPY --from=builder /framecast-worker .

# Usuário não-root para segurança
RUN addgroup -S worker && adduser -S worker -G worker
USER worker

ENTRYPOINT ["/app/framecast-worker"]
