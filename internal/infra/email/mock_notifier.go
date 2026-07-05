package email

import "context"

// MockNotifier captura notificações enviadas — usado em testes.
type MockNotifier struct {
	SuccessCalls []NotifyCall
	FailureCalls []NotifyCall
}

// NotifyCall representa uma chamada capturada pelo MockNotifier.
type NotifyCall struct {
	ToEmail     string
	VideoID     string
	Detail      string // filename (sucesso) ou errMsg (falha)
	DownloadURL string // só preenchido em SendSuccess
}

func (m *MockNotifier) SendSuccess(_ context.Context, toEmail, videoID, filename, downloadURL string) {
	m.SuccessCalls = append(m.SuccessCalls, NotifyCall{ToEmail: toEmail, VideoID: videoID, Detail: filename, DownloadURL: downloadURL})
}

func (m *MockNotifier) SendFailure(_ context.Context, toEmail, videoID, errMsg string) {
	m.FailureCalls = append(m.FailureCalls, NotifyCall{ToEmail: toEmail, VideoID: videoID, Detail: errMsg})
}

func (m *MockNotifier) Reset() {
	m.SuccessCalls = nil
	m.FailureCalls = nil
}
