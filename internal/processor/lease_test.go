package processor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const leaseSelectSQL = `SELECT id, status, worker_id, attempt, user_id, original_name, s3_key_raw, last_heartbeat_at\s+FROM videos WHERE id = \$1 FOR UPDATE`
const leaseUpdateSQL = `UPDATE videos SET worker_id = \$1, attempt = attempt \+ 1,\s+last_heartbeat_at = NOW\(\), updated_at = NOW\(\)\s+WHERE id = \$2`

var leaseColumns = []string{"id", "status", "worker_id", "attempt", "user_id", "original_name", "s3_key_raw", "last_heartbeat_at"}

func TestAcquireLease_VideoJaDone(t *testing.T) {
	db, mock := newSQLMockDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(leaseSelectSQL).WithArgs("v1").
		WillReturnRows(sqlmock.NewRows(leaseColumns).AddRow("v1", "DONE", nil, 1, "u1", "f.mp4", "raw/u1/v1/orig", nil))
	mock.ExpectRollback()

	_, err := acquireLease(context.Background(), db, "v1", "worker-1")
	require.ErrorIs(t, err, errAlreadyDone)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAcquireLease_VideoJaError(t *testing.T) {
	db, mock := newSQLMockDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(leaseSelectSQL).WithArgs("v1").
		WillReturnRows(sqlmock.NewRows(leaseColumns).AddRow("v1", "ERROR", nil, 2, "u1", "f.mp4", "raw/u1/v1/orig", nil))
	mock.ExpectRollback()

	_, err := acquireLease(context.Background(), db, "v1", "worker-1")
	require.ErrorIs(t, err, errAlreadyError)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAcquireLease_HeldPorWorkerVivo(t *testing.T) {
	db, mock := newSQLMockDB(t)
	freshHeartbeat := time.Now().Add(-30 * time.Second) // dentro do leaseTTL (3min)
	mock.ExpectBegin()
	mock.ExpectQuery(leaseSelectSQL).WithArgs("v1").
		WillReturnRows(sqlmock.NewRows(leaseColumns).AddRow("v1", "PROCESSING", "outro-worker", 1, "u1", "f.mp4", "raw/u1/v1/orig", freshHeartbeat))
	mock.ExpectRollback()

	_, err := acquireLease(context.Background(), db, "v1", "worker-1")
	require.ErrorIs(t, err, errLeaseHeld)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAcquireLease_PendingAdquireComSucesso(t *testing.T) {
	db, mock := newSQLMockDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(leaseSelectSQL).WithArgs("v1").
		WillReturnRows(sqlmock.NewRows(leaseColumns).AddRow("v1", "PENDING", nil, 0, "u1", "f.mp4", "raw/u1/v1/orig", nil))
	mock.ExpectExec(leaseUpdateSQL).WithArgs("worker-1", "v1").WillReturnResult(sqlmockResult(1))
	mock.ExpectCommit()

	row, err := acquireLease(context.Background(), db, "v1", "worker-1")
	require.NoError(t, err)
	assert.Equal(t, 1, row.Attempt) // 0 + incremento refletido localmente
	assert.Equal(t, "u1", row.UserID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAcquireLease_ProcessingComHeartbeatNulo_Reassume(t *testing.T) {
	db, mock := newSQLMockDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(leaseSelectSQL).WithArgs("v1").
		WillReturnRows(sqlmock.NewRows(leaseColumns).AddRow("v1", "PROCESSING", nil, 1, "u1", "f.mp4", "raw/u1/v1/orig", nil))
	mock.ExpectExec(leaseUpdateSQL).WithArgs("worker-1", "v1").WillReturnResult(sqlmockResult(1))
	mock.ExpectCommit()

	row, err := acquireLease(context.Background(), db, "v1", "worker-1")
	require.NoError(t, err)
	assert.Equal(t, 2, row.Attempt)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAcquireLease_ProcessingComHeartbeatVelho_Reassume(t *testing.T) {
	db, mock := newSQLMockDB(t)
	staleHeartbeat := time.Now().Add(-10 * time.Minute) // fora do leaseTTL (3min)
	mock.ExpectBegin()
	mock.ExpectQuery(leaseSelectSQL).WithArgs("v1").
		WillReturnRows(sqlmock.NewRows(leaseColumns).AddRow("v1", "PROCESSING", "worker-morto", 3, "u1", "f.mp4", "raw/u1/v1/orig", staleHeartbeat))
	mock.ExpectExec(leaseUpdateSQL).WithArgs("worker-1", "v1").WillReturnResult(sqlmockResult(1))
	mock.ExpectCommit()

	row, err := acquireLease(context.Background(), db, "v1", "worker-1")
	require.NoError(t, err)
	assert.Equal(t, 4, row.Attempt)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAcquireLease_VideoNaoEncontrado(t *testing.T) {
	db, mock := newSQLMockDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(leaseSelectSQL).WithArgs("v-inexistente").
		WillReturnRows(sqlmock.NewRows(leaseColumns))
	mock.ExpectRollback()

	_, err := acquireLease(context.Background(), db, "v-inexistente", "worker-1")
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAcquireLease_ErroNaQuerySelect(t *testing.T) {
	db, mock := newSQLMockDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(leaseSelectSQL).WithArgs("v1").
		WillReturnError(errors.New("conexão perdida"))
	mock.ExpectRollback()

	_, err := acquireLease(context.Background(), db, "v1", "worker-1")
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAcquireLease_ErroNoUpdate(t *testing.T) {
	db, mock := newSQLMockDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(leaseSelectSQL).WithArgs("v1").
		WillReturnRows(sqlmock.NewRows(leaseColumns).AddRow("v1", "PENDING", nil, 0, "u1", "f.mp4", "raw/u1/v1/orig", nil))
	mock.ExpectExec(leaseUpdateSQL).WithArgs("worker-1", "v1").WillReturnError(errors.New("connection lost"))
	mock.ExpectRollback()

	_, err := acquireLease(context.Background(), db, "v1", "worker-1")
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
