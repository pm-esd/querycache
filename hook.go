package querycache

import (
	"context"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redis/v8/internal"
	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

type OpenTelemetryHook struct{}

var _ redis.Hook = OpenTelemetryHook{}

func (OpenTelemetryHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	b := make([]byte, 32)
	b = appendCmd(b, cmd)

	if ctx == nil {
		ctx = context.Background()
	}

	if trace {
		if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
			parentCtx := parentSpan.Context()
			span := opentracing.StartSpan("redis", opentracing.ChildOf(parentCtx))
			ext.SpanKindRPCClient.Set(span)
			ext.PeerService.Set(span, "redis")
			span.SetTag("redis.cmd", internal.String(b))
			ctx = opentracing.ContextWithSpan(ctx, span)
		}
	}
	return ctx, nil
}

func (OpenTelemetryHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	if trace {
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	}
	return nil
}

func (OpenTelemetryHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	if trace {

		const numCmdLimit = 100
		const numNameLimit = 10

		seen := make(map[string]struct{}, len(cmds))
		unqNames := make([]string, 0, len(cmds))

		b := make([]byte, 0, 32*len(cmds))

		for i, cmd := range cmds {
			if i > numCmdLimit {
				break
			}

			if i > 0 {
				b = append(b, '\n')
			}
			b = appendCmd(b, cmd)

			if len(unqNames) >= numNameLimit {
				continue
			}

			name := cmd.FullName()
			if _, ok := seen[name]; !ok {
				seen[name] = struct{}{}
				unqNames = append(unqNames, name)
			}
		}

		if parentSpan := opentracing.SpanFromContext(ctx); parentSpan != nil {
			parentCtx := parentSpan.Context()
			span := opentracing.StartSpan("redis", opentracing.ChildOf(parentCtx))
			ext.SpanKindRPCClient.Set(span)
			ext.PeerService.Set(span, "redis")
			span.SetTag("redis.cmds", internal.String(b))
			ctx = opentracing.ContextWithSpan(ctx, span)
		}
	}

	return ctx, nil
}

func (OpenTelemetryHook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	if trace {
		span := opentracing.SpanFromContext(ctx)
		defer span.Finish()
	}
	return nil
}

func appendCmd(b []byte, cmd redis.Cmder) []byte {
	const lenLimit = 64

	for i, arg := range cmd.Args() {
		if i > 0 {
			b = append(b, ' ')
		}

		start := len(b)
		b = internal.AppendArg(b, arg)
		if len(b)-start > lenLimit {
			b = append(b[:start+lenLimit], "..."...)
		}
	}

	if err := cmd.Err(); err != nil {
		b = append(b, ": "...)
		b = append(b, err.Error()...)
	}

	return b
}
