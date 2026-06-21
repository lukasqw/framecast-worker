package observability

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestInitOTel_Desabilitado_RetornaNoOp(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	shutdown, err := InitOTel(context.Background())
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	assert.NoError(t, shutdown(context.Background()))
}

func TestTracerMeterSpanWorker_FuncionamSemInit(t *testing.T) {
	assert.NotNil(t, Tracer())
	assert.NotNil(t, Meter())

	ctx, span := SpanWorker(context.Background(), "op-teste")
	require.NotNil(t, ctx)
	require.NotNil(t, span)
	span.End()
}

func TestInitMetrics_RegistraInstrumentos(t *testing.T) {
	meter := noop.NewMeterProvider().Meter("test")
	err := initMetrics(meter)
	require.NoError(t, err)

	assert.NotNil(t, videoProcessedCounter)
	assert.NotNil(t, videoProcessingHisto)
	assert.NotNil(t, videoFrameCountHisto)
	assert.NotNil(t, ffmpegDurationHisto)
}

func TestRecordFuncs_SemInstrumentos_NaoPanica(t *testing.T) {
	origCounter, origHisto, origFrame, origFFmpeg := videoProcessedCounter, videoProcessingHisto, videoFrameCountHisto, ffmpegDurationHisto
	videoProcessedCounter, videoProcessingHisto, videoFrameCountHisto, ffmpegDurationHisto = nil, nil, nil, nil
	defer func() {
		videoProcessedCounter, videoProcessingHisto, videoFrameCountHisto, ffmpegDurationHisto = origCounter, origHisto, origFrame, origFFmpeg
	}()

	assert.NotPanics(t, func() {
		RecordVideoProcessed(context.Background(), "done")
		RecordVideoProcessingDuration(context.Background(), 1.5, "done")
		RecordFrameCount(context.Background(), 10)
		RecordFFmpegDuration(context.Background(), 2.5)
	})
}

func TestRecordFuncs_ComInstrumentos_NaoPanica(t *testing.T) {
	meter := noop.NewMeterProvider().Meter("test")
	require.NoError(t, initMetrics(meter))

	assert.NotPanics(t, func() {
		RecordVideoProcessed(context.Background(), "done")
		RecordVideoProcessingDuration(context.Background(), 1.5, "done")
		RecordFrameCount(context.Background(), 10)
		RecordFFmpegDuration(context.Background(), 2.5)
	})
}
