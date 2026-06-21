package consumer

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/require"
)

func TestParse_Valido(t *testing.T) {
	body := `{"video_id":"v1","user_id":"u1","s3_key":"raw/u1/v1/orig","bucket":"raw","output_bucket":"out"}`
	msg, err := parse(sqstypes.Message{Body: aws.String(body)})
	require.NoError(t, err)
	require.Equal(t, "v1", msg.VideoID)
	require.Equal(t, "u1", msg.UserID)
	require.Equal(t, "raw/u1/v1/orig", msg.S3Key)
	require.Equal(t, "raw", msg.Bucket)
	require.Equal(t, "out", msg.OutputBucket)
}

func TestParse_JSONInvalido(t *testing.T) {
	_, err := parse(sqstypes.Message{Body: aws.String("{invalido")})
	require.Error(t, err)
}

func TestParse_BodyNil(t *testing.T) {
	msg, err := parse(sqstypes.Message{})
	require.NoError(t, err)
	require.Nil(t, msg)
}
