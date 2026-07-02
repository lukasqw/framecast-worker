package consumer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSQS struct {
	mu sync.Mutex

	receiveQueue [][]sqstypes.Message
	receiveCalls int
	receiveErr   error
	deleteErr    error

	deletedReceipts []string
}

func (f *fakeSQS) ReceiveMessage(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.receiveCalls++
	if f.receiveErr != nil {
		return nil, f.receiveErr
	}
	if len(f.receiveQueue) == 0 {
		return &sqs.ReceiveMessageOutput{}, nil
	}
	msgs := f.receiveQueue[0]
	f.receiveQueue = f.receiveQueue[1:]
	return &sqs.ReceiveMessageOutput{Messages: msgs}, nil
}

func (f *fakeSQS) DeleteMessage(_ context.Context, params *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.deleteErr != nil {
		return nil, f.deleteErr
	}
	f.deletedReceipts = append(f.deletedReceipts, aws.ToString(params.ReceiptHandle))
	return &sqs.DeleteMessageOutput{}, nil
}

func (f *fakeSQS) deleted() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.deletedReceipts))
	copy(out, f.deletedReceipts)
	return out
}

type fakeHandler struct {
	mu          sync.Mutex
	processed   []string
	err         error
	delay       time.Duration
	panicOn     string
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func (h *fakeHandler) Process(ctx context.Context, msg *Message, receiptHandle string) error {
	cur := h.inFlight.Add(1)
	defer h.inFlight.Add(-1)
	for {
		max := h.maxInFlight.Load()
		if cur <= max || h.maxInFlight.CompareAndSwap(max, cur) {
			break
		}
	}

	if h.delay > 0 {
		time.Sleep(h.delay)
	}

	if h.panicOn != "" && msg.VideoID == h.panicOn {
		panic("falha simulada no processamento de " + msg.VideoID)
	}

	h.mu.Lock()
	h.processed = append(h.processed, msg.VideoID)
	h.mu.Unlock()
	return h.err
}

func validMsg(videoID, receipt string) sqstypes.Message {
	body := `{"video_id":"` + videoID + `","user_id":"u1","s3_key":"raw/u1/` + videoID + `/orig","bucket":"raw","output_bucket":"out"}`
	return sqstypes.Message{Body: aws.String(body), ReceiptHandle: aws.String(receipt)}
}

func TestConsumer_ProcessaMensagemValida(t *testing.T) {
	sqsFake := &fakeSQS{receiveQueue: [][]sqstypes.Message{
		{validMsg("v1", "r1")},
	}}
	handler := &fakeHandler{}
	c := New(sqsFake, "queue-url", 2, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	handler.mu.Lock()
	defer handler.mu.Unlock()
	assert.Contains(t, handler.processed, "v1")
}

func TestConsumer_DescartaMensagemInvalida(t *testing.T) {
	sqsFake := &fakeSQS{receiveQueue: [][]sqstypes.Message{
		{{Body: aws.String("{invalido"), ReceiptHandle: aws.String("bad-receipt")}},
	}}
	handler := &fakeHandler{}
	c := New(sqsFake, "queue-url", 1, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	assert.Contains(t, sqsFake.deleted(), "bad-receipt")
	handler.mu.Lock()
	defer handler.mu.Unlock()
	assert.Empty(t, handler.processed)
}

func TestConsumer_PanicEmUmaMensagemNaoDerrubaOutras(t *testing.T) {
	sqsFake := &fakeSQS{receiveQueue: [][]sqstypes.Message{
		{validMsg("v1", "r1"), validMsg("v2", "r2")},
	}}
	handler := &fakeHandler{panicOn: "v1"}
	c := New(sqsFake, "queue-url", 2, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	handler.mu.Lock()
	defer handler.mu.Unlock()
	assert.Contains(t, handler.processed, "v2")
	assert.NotContains(t, sqsFake.deleted(), "r1", "mensagem que causou panic não deve ser confirmada — SQS deve reentregar")
}

func TestConsumer_RespeitaCapDeConcorrencia(t *testing.T) {
	sqsFake := &fakeSQS{receiveQueue: [][]sqstypes.Message{
		{validMsg("v1", "r1"), validMsg("v2", "r2")},
		{validMsg("v3", "r3"), validMsg("v4", "r4")},
	}}
	handler := &fakeHandler{delay: 30 * time.Millisecond}
	c := New(sqsFake, "queue-url", 1, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	assert.LessOrEqual(t, handler.maxInFlight.Load(), int32(1))
}

func TestConsumer_EncerraGraciosamenteNoContextoDoneSemMensagens(t *testing.T) {
	sqsFake := &fakeSQS{}
	handler := &fakeHandler{}
	c := New(sqsFake, "queue-url", 1, handler)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run não retornou após cancelamento do contexto")
	}
}

func TestConsumer_AguardaProcessamentoEmAndamentoAoEncerrar(t *testing.T) {
	sqsFake := &fakeSQS{receiveQueue: [][]sqstypes.Message{
		{validMsg("v1", "r1")},
	}}
	handler := &fakeHandler{delay: 100 * time.Millisecond}
	c := New(sqsFake, "queue-url", 1, handler)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()

	// Cancela logo depois de disparar o processamento — Run deve esperar wg.Wait().
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run não aguardou o processamento em andamento")
	}

	handler.mu.Lock()
	defer handler.mu.Unlock()
	assert.Contains(t, handler.processed, "v1")
}

func TestConsumer_DeleteComReceiptHandleNil(t *testing.T) {
	c := &Consumer{}
	c.delete(context.Background(), nil) // não deve panicar nem chamar o client
}

func TestConsumer_DeleteComErroDoSQS_ApenasLoga(t *testing.T) {
	sqsFake := &fakeSQS{}
	sqsFake.deleteErr = assertCustomError{}
	c := &Consumer{sqs: sqsFake, queueURL: "queue-url"}

	receipt := "r1"
	assert.NotPanics(t, func() { c.delete(context.Background(), &receipt) })
}

func TestConsumer_ErroDeReceiveNaoEncerraLoop(t *testing.T) {
	sqsFake := &fakeSQS{receiveErr: assertCustomError{}}
	handler := &fakeHandler{}
	c := New(sqsFake, "queue-url", 1, handler)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	require.GreaterOrEqual(t, sqsFake.receiveCalls, 1)
}

type assertCustomError struct{}

func (assertCustomError) Error() string { return "erro simulado de receive" }
