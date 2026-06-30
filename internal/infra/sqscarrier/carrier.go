package sqscarrier

import (
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"go.opentelemetry.io/otel/propagation"
)

// PropagationKeys devem ser listados em ReceiveMessageInput.MessageAttributeNames.
var PropagationKeys = []string{"traceparent", "tracestate"}

// InjectCarrier adapta map[string]MessageAttributeValue para injeção de trace context (api → SQS).
type InjectCarrier map[string]sqstypes.MessageAttributeValue

func (c InjectCarrier) Set(key, value string) {
	v := value
	c[key] = sqstypes.MessageAttributeValue{DataType: strPtr("String"), StringValue: &v}
}
func (c InjectCarrier) Get(key string) string { return "" }
func (c InjectCarrier) Keys() []string        { return nil }

// ExtractCarrier adapta map[string]MessageAttributeValue para extração de trace context (SQS → worker).
type ExtractCarrier map[string]sqstypes.MessageAttributeValue

func (c ExtractCarrier) Get(key string) string {
	if v, ok := c[key]; ok && v.StringValue != nil {
		return *v.StringValue
	}
	return ""
}
func (c ExtractCarrier) Set(key, value string) {}
func (c ExtractCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

var _ propagation.TextMapCarrier = InjectCarrier{}
var _ propagation.TextMapCarrier = ExtractCarrier{}

func strPtr(s string) *string { return &s }
