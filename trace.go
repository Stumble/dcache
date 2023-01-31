package dcache

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// If not hit attribute was specified, the request is blocked by single-flight
// and then it means it copied value from 'other flight'.
type hitFrom string

const (
	// TracerName is the name of the tracer. This will be used as an attribute
	// on each span.
	tracerName = "github.com/stumble/dcache"

	// InstrumentationVersion is the version of the wpgx library. This will
	// be used as an attribute on each span.
	instrumentationVersion = "v0.0.1"

	attributeParam = "dcache.params"
	attributeHit   = "dcache.hit"

	hitDB    hitFrom = "db"
	hitMem   hitFrom = "mem"
	hitRedis hitFrom = "redis"
)

type tracer struct {
	tracer trace.Tracer
	attrs  []attribute.KeyValue
}

// NewTracer returns a new Tracer.
func newTracer() *tracer {
	return &tracer{
		tracer: otel.GetTracerProvider().Tracer(
			tracerName, trace.WithInstrumentationVersion(instrumentationVersion)),
		attrs: []attribute.KeyValue{
			attribute.Key("DAO").String("dcache"),
		},
	}
}

func recordError(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

// TraceStart is called at the beginning of GetWithTtl, Set, and Invalidate calls.
// The returned context is used for the rest of the call and will be passed to TraceQueryEnd.
func (t *tracer) TraceStart(ctx context.Context, opName string, params []string) context.Context {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx
	}

	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(t.attrs...),
		trace.WithAttributes(attribute.Key("Function").String(opName)),
		trace.WithAttributes(attribute.Key(attributeParam).StringSlice(params)),
	}
	ctx, _ = t.tracer.Start(ctx, opName, opts...)
	return ctx
}

// TraceEnd is called at the end of Query, QueryRow, and Exec calls.
func (t *tracer) TraceHitFrom(ctx context.Context, hit hitFrom) {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return
	}
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.Key(attributeHit).String(string(hit)))
}

// TraceEnd is called at the end of Query, QueryRow, and Exec calls.
func (t *tracer) TraceEnd(ctx context.Context, err error) {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return
	}
	span := trace.SpanFromContext(ctx)
	recordError(span, err)
	span.End()
}
