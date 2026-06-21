package processor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
)

type fakeHeartbeatSQS struct {
	calls     atomic.Int32
	err       error
	deleteErr error

	mu              sync.Mutex
	deletedReceipts []string
}

func (f *fakeHeartbeatSQS) DeleteMessage(_ context.Context, params *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedReceipts = append(f.deletedReceipts, aws.ToString(params.ReceiptHandle))
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeHeartbeatSQS) deletedReceiptsList() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.deletedReceipts))
	copy(out, f.deletedReceipts)
	return out
}

func (f *fakeHeartbeatSQS) ChangeMessageVisibility(context.Context, *sqs.ChangeMessageVisibilityInput, ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	f.calls.Add(1)
	return &sqs.ChangeMessageVisibilityOutput{}, f.err
}

func TestHeartbeat_RenovaPeriodicamente(t *testing.T) {
	orig := heartbeatInterval
	heartbeatInterval = 5 * time.Millisecond
	defer func() { heartbeatInterval = orig }()

	db, mock := newSQLMockDB(t)
	// Registra a expectativa várias vezes: com um ticker de 5ms, mais de um tick
	// pode disparar antes de stop() ser observado pela goroutine.
	for range 20 {
		mock.ExpectExec(`UPDATE videos SET last_heartbeat_at = NOW\(\) WHERE id = \$1`).
			WithArgs("v1").
			WillReturnResult(sqlmockResult(1))
	}

	sqsFake := &fakeHeartbeatSQS{}

	stop := startHeartbeat(context.Background(), sqsFake, "queue-url", "receipt-1", db, "v1")
	assert.Eventually(t, func() bool { return sqsFake.calls.Load() >= 1 }, time.Second, 5*time.Millisecond)
	stop()
	// Dá tempo da goroutine observar o cancelamento antes do t.Cleanup fechar o DB
	// mockado — evita que ela vaze logs/erros para o teste seguinte.
	time.Sleep(20 * time.Millisecond)
}

func TestHeartbeat_ErroNoChangeVisibility_ApenasLoga(t *testing.T) {
	orig := heartbeatInterval
	heartbeatInterval = 5 * time.Millisecond
	defer func() { heartbeatInterval = orig }()

	db, mock := newSQLMockDB(t)
	for range 20 {
		mock.ExpectExec(`UPDATE videos SET last_heartbeat_at = NOW\(\) WHERE id = \$1`).
			WithArgs("v1").
			WillReturnResult(sqlmockResult(1))
	}

	sqsFake := &fakeHeartbeatSQS{err: errors.New("visibility indisponível")}

	stop := startHeartbeat(context.Background(), sqsFake, "queue-url", "receipt-1", db, "v1")
	assert.Eventually(t, func() bool { return sqsFake.calls.Load() >= 1 }, time.Second, 5*time.Millisecond)
	stop()
	time.Sleep(20 * time.Millisecond)
}

func TestHeartbeat_CancelaSemNovoTick(t *testing.T) {
	orig := heartbeatInterval
	heartbeatInterval = 1 * time.Hour
	defer func() { heartbeatInterval = orig }()

	db, _ := newSQLMockDB(t)
	sqsFake := &fakeHeartbeatSQS{}

	stop := startHeartbeat(context.Background(), sqsFake, "queue-url", "receipt-1", db, "v1")
	stop()
	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, int32(0), sqsFake.calls.Load())
}
