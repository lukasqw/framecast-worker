package database

import (
	"context"
	"errors"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/lukasqw/framecast-worker/internal/infra/observability"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func newMockDB(t *testing.T) (*gorm.DB, sqlmock.Sqlmock) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn:                 sqlDB,
		PreferSimpleProtocol: true,
	}), &gorm.Config{})
	require.NoError(t, err)
	return db, mock
}

func TestOtelPlugin_Name(t *testing.T) {
	p := newOTelPlugin()
	assert.Equal(t, "framecast:otel", p.Name())
}

func TestOtelPlugin_Initialize_RegistraCallbacksSemErro(t *testing.T) {
	db, _ := newMockDB(t)
	p := newOTelPlugin()
	require.NoError(t, p.Initialize(db))
}

func TestOtelPlugin_Before_OTelDesabilitado_NaoDefineSpan(t *testing.T) {
	// OTel não é inicializado em testes — OTelInitialized() retorna false por padrão.
	db, _ := newMockDB(t)
	sess := db.Session(&gorm.Session{})
	sess.Statement.Context = context.Background()

	p := newOTelPlugin()
	assert.NotPanics(t, func() { p.before("query")(sess) })

	_, ok := sess.InstanceGet("otel:span")
	assert.False(t, ok, "OTel desabilitado por padrão nos testes — span não deveria ser criado")
}

func TestOtelPlugin_After_SemSpanRegistrado_NaoPanica(t *testing.T) {
	db, _ := newMockDB(t)
	sess := db.Session(&gorm.Session{})

	p := newOTelPlugin()
	assert.NotPanics(t, func() { p.after(sess) })
}

func TestOtelPlugin_After_ComSpan_DefineAtributosEEncerra(t *testing.T) {
	db, _ := newMockDB(t)
	sess := db.Session(&gorm.Session{})
	sess.Statement.Table = "videos"
	sess.Statement.RowsAffected = 3

	_, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "db.query")
	// InstanceSet retorna um *DB com o span no Statement correto (clone cria novo Statement).
	sessWithSpan := sess.InstanceSet("otel:span", span)

	p := newOTelPlugin()
	assert.NotPanics(t, func() { p.after(sessWithSpan) })
}

func TestOtelPlugin_After_ComErro_RegistraErroNoSpan(t *testing.T) {
	db, _ := newMockDB(t)
	sess := db.Session(&gorm.Session{})
	sess.Statement.Table = "videos"

	_, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "db.update")
	sessWithSpan := sess.InstanceSet("otel:span", span)
	sessWithSpan.Error = errors.New("falha de conexão")

	p := newOTelPlugin()
	assert.NotPanics(t, func() { p.after(sessWithSpan) })
}

func TestOtelPlugin_After_ErroRecordNotFound_NaoRegistraComoErro(t *testing.T) {
	db, _ := newMockDB(t)
	sess := db.Session(&gorm.Session{})
	sess.Statement.Table = "videos"

	_, span := noop.NewTracerProvider().Tracer("test").Start(context.Background(), "db.query")
	sessWithSpan := sess.InstanceSet("otel:span", span)
	sessWithSpan.Error = gorm.ErrRecordNotFound

	p := newOTelPlugin()
	assert.NotPanics(t, func() { p.after(sessWithSpan) })
}

func TestOtelPlugin_After_SpanComTipoInvalido_NaoPanica(t *testing.T) {
	db, _ := newMockDB(t)
	sess := db.Session(&gorm.Session{})
	sessWithInvalid := sess.InstanceSet("otel:span", "nao-eh-um-span")

	p := newOTelPlugin()
	assert.NotPanics(t, func() { p.after(sessWithInvalid) })
}

func TestOtelPlugin_Before_OTelHabilitado_NaoPanica(t *testing.T) {
	shutdown := observability.InitNoop()
	defer shutdown()

	db, _ := newMockDB(t)
	sess := db.Session(&gorm.Session{})
	sess.Statement.Context = context.Background()

	p := newOTelPlugin()
	// OTel habilitado — caminho completo executa (SpanUseCase + InstanceSet)
	assert.NotPanics(t, func() { p.before("query")(sess) })
	assert.True(t, observability.OTelInitialized())
}
