package broker

import (
	"context"

	"github.com/nats-io/nats.go"
)

type Interceptor interface {
	apply(context.Context, *nats.Msg) (context.Context, *nats.Msg) 
}

type pubInterceptor interface {
	pubInterceptor()
	Interceptor
}

type subInterceptor interface {
	subInterceptor()
	Interceptor
}

type InterceptorChain struct {
	pub []pubInterceptor
	sub []subInterceptor
}

type pubInterceptorHandler struct {
	function func(context.Context, *nats.Msg) (context.Context, *nats.Msg)
}

type subInterceptorHandler struct {
	function func(context.Context, *nats.Msg) (context.Context, *nats.Msg)
}

func (i *pubInterceptorHandler) pubInterceptor() {}


func NewPubInterceptor(f func(context.Context, *nats.Msg) (context.Context, *nats.Msg)) Interceptor {
	return &pubInterceptorHandler{
		function: f,
	}
}

func (i *pubInterceptorHandler) apply(ctx context.Context, msg *nats.Msg) (context.Context, *nats.Msg) {
	return i.function(ctx, msg)
}

func (i *subInterceptorHandler) subInterceptor() {}

func (i *subInterceptorHandler) apply(ctx context.Context, msg *nats.Msg) (context.Context, *nats.Msg) {
	return i.function(ctx, msg)
}

func NewSubInterceptor(f func(context.Context, *nats.Msg) (context.Context, *nats.Msg)) Interceptor {
	return &subInterceptorHandler{
		function: f,
	}
}

func NewInterceptorChain(i... Interceptor) *InterceptorChain {
	chain := &InterceptorChain{
		pub: []pubInterceptor{},
		sub: []subInterceptor{},
	}
	for _, curent := range i {
		if pub, ok := curent.(pubInterceptor); ok {
			chain.pub = append(chain.pub, pub)
		}
		if sub, ok := curent.(subInterceptor); ok {
			chain.sub = append(chain.sub, sub)
		}
	}
	return chain
}

func (i *InterceptorChain) applyPub(ctx context.Context, msg *nats.Msg) (context.Context, *nats.Msg) {
	for _, curent := range i.pub {
		ctx, msg = curent.apply(ctx, msg)
	}
	return ctx, msg
}

func (i *InterceptorChain) applySub(ctx context.Context, msg *nats.Msg) (context.Context, *nats.Msg) {
	for _, curent := range i.sub {
		ctx, msg = curent.apply(ctx, msg)
	}
	return ctx, msg
}
