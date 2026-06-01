package database

import (
	"fmt"
	"log/slog"

	appconfig "github.com/lukasqw/framecast-worker/internal/config"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// Connect abre a conexão com o banco. O worker não executa AutoMigrate —
// o schema é gerenciado exclusivamente pelo framecast-api.
func Connect(cfg *appconfig.Config) (*gorm.DB, error) {
	logLevel := gormlogger.Silent
	if cfg.AppEnv == "dev" {
		logLevel = gormlogger.Warn
	}

	db, err := gorm.Open(postgres.Open(cfg.DatabaseURL), &gorm.Config{
		Logger: gormlogger.Default.LogMode(logLevel),
	})
	if err != nil {
		return nil, fmt.Errorf("falha ao conectar ao banco: %w", err)
	}

	slog.Info("banco de dados conectado")
	return db, nil
}
