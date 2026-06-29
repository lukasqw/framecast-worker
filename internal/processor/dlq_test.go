package processor

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/lukasqw/framecast-worker/internal/consumer"
	"github.com/lukasqw/framecast-worker/internal/infra/email"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type fakeDLQSQS struct {
	deletedReceipts []string
	deleteErr       error
}

func (f *fakeDLQSQS) DeleteMessage(_ context.Context, params *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	f.deletedReceipts = append(f.deletedReceipts, aws.ToString(params.ReceiptHandle))
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeDLQSQS) ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func newDLQHandlerForTest(db *gorm.DB, sqsFake sqsAPI, notif *email.MockNotifier) *DLQHandler {
	return &DLQHandler{db: db, sqs: sqsFake, queueURL: "dlq-url", notifier: notif}
}

func TestDLQHandler_VideoJaFinalizado(t *testing.T) {
	db, mock := newSQLMockDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT status, user_id FROM videos WHERE id = \$1 FOR UPDATE`).
		WithArgs("v1").
		WillReturnRows(sqlmock.NewRows([]string{"status", "user_id"}).AddRow("DONE", "u1"))
	mock.ExpectCommit()

	sqsFake := &fakeDLQSQS{}
	notif := &email.MockNotifier{}
	h := newDLQHandlerForTest(db, sqsFake, notif)

	err := h.Process(context.Background(), &consumer.Message{VideoID: "v1"}, "receipt-1")
	require.NoError(t, err)
	assert.Contains(t, sqsFake.deletedReceipts, "receipt-1")
	assert.Empty(t, notif.FailureCalls)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDLQHandler_MarcaErrorENotifica(t *testing.T) {
	db, mock := newSQLMockDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT status, user_id FROM videos WHERE id = \$1 FOR UPDATE`).
		WithArgs("v1").
		WillReturnRows(sqlmock.NewRows([]string{"status", "user_id"}).AddRow("PROCESSING", "u1"))
	mock.ExpectExec(`UPDATE videos SET status='ERROR', error_message=\$1, worker_id=NULL, updated_at=NOW\(\) WHERE id=\$2`).
		WithArgs(dlqErrorReason, "v1").
		WillReturnResult(sqlmockResult(1))
	mock.ExpectQuery(`SELECT email FROM users WHERE id = \$1`).
		WithArgs("u1").
		WillReturnRows(sqlmock.NewRows([]string{"email"}).AddRow("user@example.com"))
	mock.ExpectCommit()

	sqsFake := &fakeDLQSQS{}
	notif := &email.MockNotifier{}
	h := newDLQHandlerForTest(db, sqsFake, notif)

	err := h.Process(context.Background(), &consumer.Message{VideoID: "v1"}, "receipt-1")
	require.NoError(t, err)
	require.Len(t, notif.FailureCalls, 1)
	assert.Equal(t, "user@example.com", notif.FailureCalls[0].ToEmail)
	assert.Contains(t, sqsFake.deletedReceipts, "receipt-1")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDLQHandler_ErroNaTransacao_NaoDeleta(t *testing.T) {
	db, mock := newSQLMockDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT status, user_id FROM videos WHERE id = \$1 FOR UPDATE`).
		WithArgs("v1").
		WillReturnError(errors.New("conexão perdida"))
	mock.ExpectRollback()

	sqsFake := &fakeDLQSQS{}
	notif := &email.MockNotifier{}
	h := newDLQHandlerForTest(db, sqsFake, notif)

	err := h.Process(context.Background(), &consumer.Message{VideoID: "v1"}, "receipt-1")
	require.Error(t, err)
	assert.Empty(t, sqsFake.deletedReceipts)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestDLQHandler_ErroAoDeletarMensagem(t *testing.T) {
	db, mock := newSQLMockDB(t)
	mock.ExpectBegin()
	mock.ExpectQuery(`SELECT status, user_id FROM videos WHERE id = \$1 FOR UPDATE`).
		WithArgs("v1").
		WillReturnRows(sqlmock.NewRows([]string{"status", "user_id"}).AddRow("DONE", "u1"))
	mock.ExpectCommit()

	sqsFake := &fakeDLQSQS{deleteErr: errors.New("sqs indisponível")}
	notif := &email.MockNotifier{}
	h := newDLQHandlerForTest(db, sqsFake, notif)

	err := h.Process(context.Background(), &consumer.Message{VideoID: "v1"}, "receipt-1")
	require.Error(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
