package processor

import (
	"testing"

	"github.com/lukasqw/framecast-worker/internal/infra/email"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_ConstroiProcessorComDependencias(t *testing.T) {
	db, _ := newSQLMockDB(t)
	s3Fake := &fakeS3Full{}
	sqsFake := &fakeHeartbeatSQS{}
	notif := &email.MockNotifier{}

	p := New(db, s3Fake, sqsFake, notif, "queue-url", 30)
	require.NotNil(t, p)
	assert.Equal(t, "queue-url", p.queueURL)
	assert.Equal(t, 30, p.ffmpegTimeoutMinutes)
	assert.NotNil(t, p.notifier)
	assert.NotNil(t, p.uploader)
	assert.NotEmpty(t, p.workerID)
}

func TestNewDLQHandler_ConstroiHandlerComDependencias(t *testing.T) {
	db, _ := newSQLMockDB(t)
	sqsFake := &fakeHeartbeatSQS{}
	notif := &email.MockNotifier{}

	h := NewDLQHandler(db, sqsFake, notif, "dlq-url")
	require.NotNil(t, h)
	assert.Equal(t, "dlq-url", h.queueURL)
	assert.NotNil(t, h.notifier)
}
