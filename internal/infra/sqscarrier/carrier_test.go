package sqscarrier

import (
	"context"
	"testing"

	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func TestInjectCarrier_Set_ArmazenaDataTypeEStringValue(t *testing.T) {
	c := InjectCarrier{}
	c.Set("traceparent", "00-abc123-def456-01")

	attr, ok := c["traceparent"]
	assert.True(t, ok)
	assert.NotNil(t, attr.DataType)
	assert.Equal(t, "String", *attr.DataType)
	assert.NotNil(t, attr.StringValue)
	assert.Equal(t, "00-abc123-def456-01", *attr.StringValue)
}

func TestInjectCarrier_Get_SempreRetornaVazio(t *testing.T) {
	c := InjectCarrier{}
	c.Set("traceparent", "val")
	assert.Equal(t, "", c.Get("traceparent"))
	assert.Equal(t, "", c.Get("chave-ausente"))
}

func TestInjectCarrier_Keys_RetornaNil(t *testing.T) {
	c := InjectCarrier{}
	c.Set("traceparent", "val")
	assert.Nil(t, c.Keys())
}

func TestExtractCarrier_Get_RetornaStringValue(t *testing.T) {
	val := "00-abc123-def456-01"
	c := ExtractCarrier{
		"traceparent": sqstypes.MessageAttributeValue{StringValue: &val},
	}
	assert.Equal(t, "00-abc123-def456-01", c.Get("traceparent"))
}

func TestExtractCarrier_Get_ChaveAusente_RetornaVazio(t *testing.T) {
	c := ExtractCarrier{}
	assert.Equal(t, "", c.Get("traceparent"))
}

func TestExtractCarrier_Get_StringValueNil_RetornaVazio(t *testing.T) {
	c := ExtractCarrier{
		"traceparent": sqstypes.MessageAttributeValue{StringValue: nil},
	}
	assert.Equal(t, "", c.Get("traceparent"))
}

func TestExtractCarrier_Set_NaoPanicaNemArmazena(t *testing.T) {
	c := ExtractCarrier{}
	assert.NotPanics(t, func() { c.Set("key", "val") })
	assert.Empty(t, c)
}

func TestExtractCarrier_Keys_RetornaTodas(t *testing.T) {
	val := "v"
	c := ExtractCarrier{
		"traceparent": sqstypes.MessageAttributeValue{StringValue: &val},
		"tracestate":  sqstypes.MessageAttributeValue{StringValue: &val},
	}
	keys := c.Keys()
	assert.Len(t, keys, 2)
	assert.ElementsMatch(t, []string{"traceparent", "tracestate"}, keys)
}

func TestExtractCarrier_Keys_VazioQuandoMapaVazio(t *testing.T) {
	c := ExtractCarrier{}
	assert.Empty(t, c.Keys())
}

func TestPropagationKeys_ContemTraceparentETracestate(t *testing.T) {
	assert.Contains(t, PropagationKeys, "traceparent")
	assert.Contains(t, PropagationKeys, "tracestate")
	assert.Len(t, PropagationKeys, 2)
}

func TestPropagationRoundTrip_InjectExtract_NaoPanica(t *testing.T) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	inject := InjectCarrier{}
	assert.NotPanics(t, func() {
		otel.GetTextMapPropagator().Inject(context.Background(), inject)
	})

	extracted := otel.GetTextMapPropagator().Extract(context.Background(), ExtractCarrier(inject))
	assert.NotNil(t, extracted)
}
