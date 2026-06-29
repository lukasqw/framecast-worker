package processor

import (
	"context"
	"testing"

	"github.com/lukasqw/framecast-worker/internal/infra/email"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMockNotifier_CapturaSuccessCalls(t *testing.T) {
	m := &email.MockNotifier{}
	m.SendSuccess(context.Background(), "user@example.com", "v1", "video.mp4")

	require.Len(t, m.SuccessCalls, 1)
	assert.Equal(t, "user@example.com", m.SuccessCalls[0].ToEmail)
	assert.Equal(t, "v1", m.SuccessCalls[0].VideoID)
	assert.Equal(t, "video.mp4", m.SuccessCalls[0].Detail)
}

func TestMockNotifier_CapturaFailureCalls(t *testing.T) {
	m := &email.MockNotifier{}
	m.SendFailure(context.Background(), "user@example.com", "v1", "ffmpeg error")

	require.Len(t, m.FailureCalls, 1)
	assert.Equal(t, "v1", m.FailureCalls[0].VideoID)
	assert.Equal(t, "ffmpeg error", m.FailureCalls[0].Detail)
}

func TestMockNotifier_Reset(t *testing.T) {
	m := &email.MockNotifier{}
	m.SendSuccess(context.Background(), "a@b.com", "v1", "f.mp4")
	m.Reset()
	assert.Empty(t, m.SuccessCalls)
	assert.Empty(t, m.FailureCalls)
}
