package utils

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/trace"
)

func InitTracer() {
	exporter, _ := stdouttrace.New(stdouttrace.WithPrettyPrint())
	bsp := trace.NewBatchSpanProcessor(exporter)
	tp := trace.NewTracerProvider(trace.WithSpanProcessor(bsp))
	otel.SetTracerProvider(tp)
}
