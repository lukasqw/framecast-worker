package consumer

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

func TestParse_Valido(t *testing.T) {
	body := `{"video_id":"v1","user_id":"u1","s3_key":"raw/u1/v1/orig","bucket":"raw","output_bucket":"out"}`
	msg, err := parse(sqstypes.Message{Body: aws.String(body)})
	if err != nil {
		t.Fatalf("erro inesperado: %v", err)
	}
	if msg.VideoID != "v1" || msg.UserID != "u1" || msg.S3Key != "raw/u1/v1/orig" ||
		msg.Bucket != "raw" || msg.OutputBucket != "out" {
		t.Fatalf("campos parseados incorretamente: %+v", msg)
	}
}

func TestParse_JSONInvalido(t *testing.T) {
	if _, err := parse(sqstypes.Message{Body: aws.String("{invalido")}); err == nil {
		t.Fatal("esperava erro para JSON inválido")
	}
}

func TestParse_BodyNil(t *testing.T) {
	msg, err := parse(sqstypes.Message{})
	if err != nil {
		t.Fatalf("body nil não deveria gerar erro: %v", err)
	}
	if msg != nil {
		t.Fatalf("body nil deveria retornar msg nil, obteve %+v", msg)
	}
}
