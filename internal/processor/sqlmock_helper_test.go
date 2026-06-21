package processor

import (
	"database/sql/driver"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// newSQLMockDB cria um *gorm.DB real sobre um driver SQL mockado (sem Postgres
// de verdade) — permite testar SELECT/UPDATE/Transaction com sqlmock.ExpectXxx.
func newSQLMockDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()

	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	gormDB, err := gorm.Open(postgres.New(postgres.Config{
		Conn:                 sqlDB,
		PreferSimpleProtocol: true,
	}), &gorm.Config{})
	require.NoError(t, err)

	return gormDB, mock
}

func sqlmockResult(rowsAffected int64) driver.Result {
	return sqlmock.NewResult(0, rowsAffected)
}
