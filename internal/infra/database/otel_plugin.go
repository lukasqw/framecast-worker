package database

import (
	"errors"

	"github.com/lukasqw/framecast-worker/internal/infra/observability"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
)

type otelPlugin struct{}

func newOTelPlugin() *otelPlugin { return &otelPlugin{} }

func (p *otelPlugin) Name() string { return "framecast:otel" }

func (p *otelPlugin) Initialize(db *gorm.DB) error {
	var errs []error
	errs = append(errs, db.Callback().Create().Before("gorm:create").Register("otel:before_create", p.before("create")))
	errs = append(errs, db.Callback().Create().After("gorm:create").Register("otel:after_create", p.after))
	errs = append(errs, db.Callback().Query().Before("gorm:query").Register("otel:before_query", p.before("query")))
	errs = append(errs, db.Callback().Query().After("gorm:query").Register("otel:after_query", p.after))
	errs = append(errs, db.Callback().Update().Before("gorm:update").Register("otel:before_update", p.before("update")))
	errs = append(errs, db.Callback().Update().After("gorm:update").Register("otel:after_update", p.after))
	errs = append(errs, db.Callback().Delete().Before("gorm:delete").Register("otel:before_delete", p.before("delete")))
	errs = append(errs, db.Callback().Delete().After("gorm:delete").Register("otel:after_delete", p.after))
	errs = append(errs, db.Callback().Raw().Before("gorm:raw").Register("otel:before_raw", p.before("raw")))
	errs = append(errs, db.Callback().Raw().After("gorm:raw").Register("otel:after_raw", p.after))
	return errors.Join(errs...)
}

func (p *otelPlugin) before(op string) func(*gorm.DB) {
	return func(db *gorm.DB) {
		if !observability.OTelInitialized() {
			return
		}
		ctx, span := observability.SpanWorker(db.Statement.Context, "db."+op)
		db.Statement.Context = ctx
		db.InstanceSet("otel:span", span)
	}
}

func (p *otelPlugin) after(db *gorm.DB) {
	val, ok := db.InstanceGet("otel:span")
	if !ok {
		return
	}
	span, ok := val.(trace.Span)
	if !ok {
		return
	}
	defer span.End()

	if db.Statement != nil {
		span.SetAttributes(
			attribute.String("db.table", db.Statement.Table),
			attribute.Int64("db.rows_affected", db.Statement.RowsAffected),
		)
	}

	if db.Error != nil && !errors.Is(db.Error, gorm.ErrRecordNotFound) {
		span.RecordError(db.Error)
		span.SetStatus(codes.Error, db.Error.Error())
	}
}
