package processor

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/lukasqw/framecast-worker/internal/consumer"
	"github.com/lukasqw/framecast-worker/internal/infra/email"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeS3Full satisfaz s3API (GetObject + manager.UploadAPIClient) — só GetObject
// é exercitado pelo Processor; os demais métodos pertencem à interface mas não
// são chamados nesses testes (o upload do ZIP passa pelo campo uploader, não s3Client).
type fakeS3Full struct {
	getObjectBody string
	getObjectErr  error
}

func (f *fakeS3Full) GetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if f.getObjectErr != nil {
		return nil, f.getObjectErr
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(f.getObjectBody))}, nil
}
func (f *fakeS3Full) PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, nil
}
func (f *fakeS3Full) UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return nil, nil
}
func (f *fakeS3Full) CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return nil, nil
}
func (f *fakeS3Full) CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return nil, nil
}
func (f *fakeS3Full) AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return nil, nil
}

const fakePresignedURL = "https://framecast-videos-output.s3.amazonaws.com/output.zip?presigned=1"

// fakePresigner satisfaz presignAPI — usado no lugar do *s3.PresignClient real
// pra validar que a URL de download chega até o e-mail de sucesso sem depender
// de credenciais AWS reais.
type fakePresigner struct {
	url string
	err error
}

func (f *fakePresigner) PresignGetObject(_ context.Context, _ *s3.GetObjectInput, _ ...func(*s3.PresignOptions)) (*v4.PresignedHTTPRequest, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &v4.PresignedHTTPRequest{URL: f.url}, nil
}

type processorTestDeps struct {
	mock      sqlmock.Sqlmock
	s3        *fakeS3Full
	up        *fakeUploader
	sqs       *fakeHeartbeatSQS
	notif     *email.MockNotifier
	presigner *fakePresigner
}

func newTestProcessor(t *testing.T) (*Processor, *processorTestDeps) {
	t.Helper()
	db, mock := newSQLMockDB(t)
	s3Fake := &fakeS3Full{getObjectBody: "video-bytes"}
	up := &fakeUploader{}
	sqsFake := &fakeHeartbeatSQS{}
	notif := &email.MockNotifier{}
	presignerFake := &fakePresigner{url: fakePresignedURL}

	p := &Processor{
		db:                   db,
		s3Client:             s3Fake,
		uploader:             up,
		presigner:            presignerFake,
		sqsClient:            sqsFake,
		queueURL:             "queue-url",
		workerID:             "worker-1",
		ffmpegTimeoutMinutes: 30,
		ffmpegFPS:            1,
		notifier:             notif,
	}
	return p, &processorTestDeps{mock: mock, s3: s3Fake, up: up, sqs: sqsFake, notif: notif, presigner: presignerFake}
}

func testMsg() *consumer.Message {
	return &consumer.Message{
		VideoID: "v1", UserID: "u1", S3Key: "raw/u1/v1/orig", Bucket: "raw", OutputBucket: "out",
	}
}

// fakeFFmpegRunnerCriaFrames cria N PNGs falsos no framesDir (último arg da
// chamada de exec) e retorna sucesso — simula extração de frames sem ffmpeg real.
func fakeFFmpegRunnerCriaFrames(n int) commandRunner {
	return func(ctx context.Context, name string, args ...string) (string, error) {
		outputPattern := args[len(args)-1]
		dir := filepath.Dir(outputPattern)
		for i := range n {
			_ = os.WriteFile(filepath.Join(dir, "frame_000"+string(rune('1'+i))+".png"), []byte("x"), 0o600)
		}
		return "", nil
	}
}

func expectLeaseAcquired(mock sqlmock.Sqlmock, videoID, workerID string) {
	mock.ExpectBegin()
	mock.ExpectQuery(leaseSelectSQL).WithArgs(videoID).
		WillReturnRows(sqlmock.NewRows(leaseColumns).AddRow(videoID, "PENDING", nil, 0, "u1", "f.mp4", "raw/u1/"+videoID+"/orig", nil))
	mock.ExpectExec(leaseUpdateSQL).WithArgs(workerID, videoID).WillReturnResult(sqlmockResult(1))
	mock.ExpectCommit()
}

func TestProcess_LeaseJaDone_DescartaMensagem(t *testing.T) {
	p, deps := newTestProcessor(t)
	deps.mock.ExpectBegin()
	deps.mock.ExpectQuery(leaseSelectSQL).WithArgs("v1").
		WillReturnRows(sqlmock.NewRows(leaseColumns).AddRow("v1", "DONE", nil, 1, "u1", "f.mp4", "raw/u1/v1/orig", nil))
	deps.mock.ExpectRollback()

	err := p.Process(context.Background(), testMsg(), "receipt-1")
	require.NoError(t, err)
	assert.Contains(t, deps.sqs.deletedReceiptsList(), "receipt-1")
	require.NoError(t, deps.mock.ExpectationsWereMet())
}

func TestProcess_LeaseHeld_DescartaDuplicata(t *testing.T) {
	p, deps := newTestProcessor(t)
	deps.mock.ExpectBegin()
	freshHeartbeat := time.Now().Add(-30 * time.Second)
	deps.mock.ExpectQuery(leaseSelectSQL).WithArgs("v1").
		WillReturnRows(sqlmock.NewRows(leaseColumns).AddRow("v1", "PROCESSING", "outro-worker", 1, "u1", "f.mp4", "raw/u1/v1/orig", freshHeartbeat))
	deps.mock.ExpectRollback()

	err := p.Process(context.Background(), testMsg(), "receipt-1")
	require.NoError(t, err)
	assert.Contains(t, deps.sqs.deletedReceiptsList(), "receipt-1")
	require.NoError(t, deps.mock.ExpectationsWereMet())
}

func TestProcess_LeaseErroGenerico_NaoDeleta(t *testing.T) {
	p, deps := newTestProcessor(t)
	deps.mock.ExpectBegin()
	deps.mock.ExpectQuery(leaseSelectSQL).WithArgs("v1").
		WillReturnRows(sqlmock.NewRows(leaseColumns))
	deps.mock.ExpectRollback()

	err := p.Process(context.Background(), testMsg(), "receipt-1")
	require.Error(t, err)
	assert.Empty(t, deps.sqs.deletedReceiptsList())
	require.NoError(t, deps.mock.ExpectationsWereMet())
}

func TestProcess_DownloadFalha_NaoDeleta(t *testing.T) {
	p, deps := newTestProcessor(t)
	expectLeaseAcquired(deps.mock, "v1", "worker-1")
	deps.s3.getObjectErr = errors.New("NoSuchKey")

	err := p.Process(context.Background(), testMsg(), "receipt-1")
	require.Error(t, err)
	assert.Empty(t, deps.sqs.deletedReceiptsList())
	require.NoError(t, deps.mock.ExpectationsWereMet())
}

func TestProcess_FFmpegFalha_MarcaErroNotificaEDeleta(t *testing.T) {
	p, deps := newTestProcessor(t)
	expectLeaseAcquired(deps.mock, "v1", "worker-1")
	deps.mock.ExpectQuery(`SELECT email FROM users WHERE id = \$1`).WithArgs("u1").
		WillReturnRows(sqlmock.NewRows([]string{"email"}).AddRow("user@example.com"))
	deps.mock.ExpectExec(`UPDATE videos SET status = 'ERROR', error_message = \$1, updated_at = NOW\(\) WHERE id = \$2`).
		WillReturnResult(sqlmockResult(1))

	withFakeRunner(t, func(ctx context.Context, name string, args ...string) (string, error) {
		return "Invalid data found", errors.New("exit status 1")
	})

	err := p.Process(context.Background(), testMsg(), "receipt-1")
	require.NoError(t, err) // não-retentável: Process devolve nil (ACK)
	require.Len(t, deps.notif.FailureCalls, 1)
	assert.Contains(t, deps.sqs.deletedReceiptsList(), "receipt-1")
	require.NoError(t, deps.mock.ExpectationsWereMet())
}

func TestProcess_ZipUploadFalha_NaoDeleta(t *testing.T) {
	p, deps := newTestProcessor(t)
	expectLeaseAcquired(deps.mock, "v1", "worker-1")
	withFakeRunner(t, fakeFFmpegRunnerCriaFrames(2))
	deps.up.err = errors.New("s3 indisponível")

	err := p.Process(context.Background(), testMsg(), "receipt-1")
	require.Error(t, err)
	assert.Empty(t, deps.sqs.deletedReceiptsList())
	require.NoError(t, deps.mock.ExpectationsWereMet())
}

func TestProcess_MarkDoneFalha_NaoDeleta(t *testing.T) {
	p, deps := newTestProcessor(t)
	expectLeaseAcquired(deps.mock, "v1", "worker-1")
	withFakeRunner(t, fakeFFmpegRunnerCriaFrames(2))
	deps.mock.ExpectExec(`UPDATE videos SET status = 'DONE', s3_key_output = \$1, frame_count = \$2,\s+worker_id = NULL, updated_at = NOW\(\) WHERE id = \$3`).
		WillReturnError(errors.New("conexão perdida"))

	err := p.Process(context.Background(), testMsg(), "receipt-1")
	require.Error(t, err)
	assert.Empty(t, deps.sqs.deletedReceiptsList())
	require.NoError(t, deps.mock.ExpectationsWereMet())
}

func TestProcess_SucessoCompleto(t *testing.T) {
	p, deps := newTestProcessor(t)
	expectLeaseAcquired(deps.mock, "v1", "worker-1")
	withFakeRunner(t, fakeFFmpegRunnerCriaFrames(3))
	deps.mock.ExpectExec(`UPDATE videos SET status = 'DONE', s3_key_output = \$1, frame_count = \$2,\s+worker_id = NULL, updated_at = NOW\(\) WHERE id = \$3`).
		WillReturnResult(sqlmockResult(1))
	deps.mock.ExpectQuery(`SELECT email FROM users WHERE id = \$1`).WithArgs("u1").
		WillReturnRows(sqlmock.NewRows([]string{"email"}).AddRow("user@example.com"))

	err := p.Process(context.Background(), testMsg(), "receipt-1")
	require.NoError(t, err)
	require.Len(t, deps.notif.SuccessCalls, 1)
	assert.Equal(t, fakePresignedURL, deps.notif.SuccessCalls[0].DownloadURL)
	assert.Contains(t, deps.sqs.deletedReceiptsList(), "receipt-1")
	require.NotNil(t, deps.up.lastInput)
	require.NoError(t, deps.mock.ExpectationsWereMet())
}

func TestProcess_PresignFalha_NaoImpedeNotificacaoDeSucesso(t *testing.T) {
	p, deps := newTestProcessor(t)
	deps.presigner.err = errors.New("credenciais expiradas")
	expectLeaseAcquired(deps.mock, "v1", "worker-1")
	withFakeRunner(t, fakeFFmpegRunnerCriaFrames(3))
	deps.mock.ExpectExec(`UPDATE videos SET status = 'DONE', s3_key_output = \$1, frame_count = \$2,\s+worker_id = NULL, updated_at = NOW\(\) WHERE id = \$3`).
		WillReturnResult(sqlmockResult(1))
	deps.mock.ExpectQuery(`SELECT email FROM users WHERE id = \$1`).WithArgs("u1").
		WillReturnRows(sqlmock.NewRows([]string{"email"}).AddRow("user@example.com"))

	err := p.Process(context.Background(), testMsg(), "receipt-1")
	require.NoError(t, err)
	require.Len(t, deps.notif.SuccessCalls, 1)
	assert.Empty(t, deps.notif.SuccessCalls[0].DownloadURL)
	assert.Contains(t, deps.sqs.deletedReceiptsList(), "receipt-1")
	require.NoError(t, deps.mock.ExpectationsWereMet())
}

func TestProcess_DeleteMessageFalha_RetornaErro(t *testing.T) {
	p, deps := newTestProcessor(t)
	deps.mock.ExpectBegin()
	deps.mock.ExpectQuery(leaseSelectSQL).WithArgs("v1").
		WillReturnRows(sqlmock.NewRows(leaseColumns).AddRow("v1", "DONE", nil, 1, "u1", "f.mp4", "raw/u1/v1/orig", nil))
	deps.mock.ExpectRollback()
	deps.sqs.deleteErr = errors.New("sqs indisponível")

	err := p.Process(context.Background(), testMsg(), "receipt-1")
	require.Error(t, err)
	require.NoError(t, deps.mock.ExpectationsWereMet())
}

func TestMarkError_FalhaNoExec_NaoPanica(t *testing.T) {
	p, deps := newTestProcessor(t)
	deps.mock.ExpectExec(`UPDATE videos SET status = 'ERROR', error_message = \$1, updated_at = NOW\(\) WHERE id = \$2`).
		WillReturnError(errors.New("conexão perdida"))

	assert.NotPanics(t, func() {
		p.markError(context.Background(), "v1", "erro de teste")
	})
	require.NoError(t, deps.mock.ExpectationsWereMet())
}

func TestMarkError_TruncaMensagemLonga(t *testing.T) {
	p, deps := newTestProcessor(t)
	longMsg := strings.Repeat("x", 600)
	deps.mock.ExpectExec(`UPDATE videos SET status = 'ERROR', error_message = \$1, updated_at = NOW\(\) WHERE id = \$2`).
		WillReturnResult(sqlmockResult(1))

	p.markError(context.Background(), "v1", longMsg)
	require.NoError(t, deps.mock.ExpectationsWereMet())
}
