package processor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

var (
	errAlreadyDone  = errors.New("vídeo já processado com sucesso")
	errAlreadyError = errors.New("vídeo já marcado como erro")
	errLeaseHeld    = errors.New("vídeo em processamento por worker vivo")
)

// leaseTTL: um vídeo PROCESSING cujo last_heartbeat_at é mais recente que isto é
// considerado sob lease vivo. Deve ser > heartbeatInterval (ver heartbeat.go) para
// tolerar um tick perdido. 3× o intervalo (1min) = 3min.
const leaseTTL = 3 * time.Minute

// videoRow representa as colunas de videos necessárias para o worker.
// Sem AutoMigrate — o schema é gerenciado pela api.
type videoRow struct {
	ID              string     `gorm:"column:id"`
	Status          string     `gorm:"column:status"`
	WorkerID        *string    `gorm:"column:worker_id"`
	Attempt         int        `gorm:"column:attempt"`
	UserID          string     `gorm:"column:user_id"`
	OriginalName    string     `gorm:"column:original_name"`
	S3KeyRaw        string     `gorm:"column:s3_key_raw"`
	LastHeartbeatAt *time.Time `gorm:"column:last_heartbeat_at"`
}

// acquireLease verifica idempotência e adquire o lease atomicamente.
//
// Discriminação por last_heartbeat_at (não por PENDING-vs-PROCESSING — desde o P1-4
// a api grava PROCESSING já no complete):
//   - DONE / ERROR              → já finalizado (skip + delete)
//   - PROCESSING + hb fresco    → worker vivo → errLeaseHeld (suprime duplicata)
//   - PENDING                   → adquire
//   - PROCESSING + hb nulo      → PROCESSING setado pela api, sem worker → adquire
//   - PROCESSING + hb velho     → worker anterior morreu → reassume
func acquireLease(ctx context.Context, db *gorm.DB, videoID, workerID string) (*videoRow, error) {
	var row videoRow

	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Raw(
			`SELECT id, status, worker_id, attempt, user_id, original_name, s3_key_raw, last_heartbeat_at
			   FROM videos WHERE id = ? FOR UPDATE`,
			videoID,
		).Scan(&row)
		if res.Error != nil {
			return fmt.Errorf("falha ao buscar vídeo para lease: %w", res.Error)
		}
		if row.ID == "" {
			return fmt.Errorf("vídeo %s não encontrado na base", videoID)
		}

		switch row.Status {
		case "DONE":
			return errAlreadyDone
		case "ERROR":
			return errAlreadyError
		}

		if row.Status == "PROCESSING" && row.LastHeartbeatAt != nil &&
			time.Since(*row.LastHeartbeatAt) < leaseTTL {
			return errLeaseHeld
		}

		return tx.Exec(
			`UPDATE videos SET worker_id = ?, attempt = attempt + 1,
			   last_heartbeat_at = NOW(), updated_at = NOW()
			  WHERE id = ?`,
			workerID, videoID,
		).Error
	})
	if err != nil {
		return nil, err
	}

	row.Attempt++ // reflete o incremento feito no UPDATE
	return &row, nil
}
