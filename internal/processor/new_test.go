package processor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_ConstroiProcessorComDependencias(t *testing.T) {
	db, _ := newSQLMockDB(t)
	s3Fake := &fakeS3Full{}
	sqsFake := &fakeHeartbeatSQS{}
	ses := &fakeSES{}

	p := New(db, s3Fake, sqsFake, ses, "queue-url", "noreply@framecast.local", "", 30)
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
	ses := &fakeSES{}

	h := NewDLQHandler(db, sqsFake, ses, "dlq-url", "noreply@framecast.local", "")
	require.NotNil(t, h)
	assert.Equal(t, "dlq-url", h.queueURL)
	assert.NotNil(t, h.notifier)
}
