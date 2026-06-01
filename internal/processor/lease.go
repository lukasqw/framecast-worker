package processor

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"
)

var (
	errAlreadyDone  = errors.New("vídeo já processado com sucesso")
	errAlreadyError = errors.New("vídeo já marcado como erro")
)

// videoRow representa as colunas de videos necessárias para o worker.
// Sem AutoMigrate — o schema é gerenciado pela api.
type videoRow struct {
	ID           string  `gorm:"column:id"`
	Status       string  `gorm:"column:status"`
	WorkerID     *string `gorm:"column:worker_id"`
	Attempt      int     `gorm:"column:attempt"`
	UserID       string  `gorm:"column:user_id"`
	OriginalName string  `gorm:"column:original_name"`
	S3KeyRaw     string  `gorm:"column:s3_key_raw"`
}

// acquireLease verifica idempotência e adquire o lease atomicamente.
// Retorna errAlreadyDone/errAlreadyError se o vídeo já foi finalizado.
// Caso contrário, atualiza worker_id e incrementa attempt dentro da mesma transação.
func acquireLease(ctx context.Context, db *gorm.DB, videoID, workerID string) (*videoRow, error) {
	var row videoRow

	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		res := tx.Raw(
			`SELECT id, status, worker_id, attempt, user_id, original_name, s3_key_raw
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

		// PENDING ou PROCESSING (worker anterior crashou — heartbeat parou,
		// visibility expirou, SQS reentregou) → adquire lease
		return tx.Exec(
			`UPDATE videos SET worker_id = ?, attempt = attempt + 1, updated_at = NOW()
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
