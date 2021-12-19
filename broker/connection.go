package broker

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/tudatravel/nats-protobuf/broker/errors"
)

type Connection interface {
	Publish(ctx context.Context, subj string, data []byte, timeout time.Duration) ([]byte, error)
	Subscribe(subj string, queue string, f func(ctx context.Context, data []byte) ([]byte, error)) error
	StreamPublish(ctx context.Context, subj string, data []byte) error
	StreamSubscribe(subj string, reply string, queue string, f func(ctx context.Context, data []byte) ([]byte, error)) error
}

type connection struct {
	interceptorChain *InterceptorChain
	nats             *nats.Conn
	js               nats.JetStreamContext
}

func (c *connection) Publish(ctx context.Context, subj string, data []byte, timeout time.Duration) ([]byte, error) {
	msg := &nats.Msg{
		Subject: subj,
		Data:    data,
	}
	_, msg = c.interceptorChain.applyPub(ctx, msg)
	msg, err := c.nats.RequestMsg(msg, timeout)
	if err != nil {
		return nil, err
	}
	// _, msg = c.interceptorChain.applySub(ctx, msg)
	return msg.Data, nil
}

func (c *connection) Subscribe(subj string, queue string, f func(ctx context.Context, data []byte) ([]byte, error)) error {
	_, err := c.nats.QueueSubscribe(subj, queue, func(msg *nats.Msg) {
		ctx := context.Background()
		ctx, msg = c.interceptorChain.applySub(ctx, msg)
		data, err := f(ctx, msg.Data)
		if err != nil {
			if errors.ErrorType(err) == errors.ErrNAK {
				_ = msg.Nak()
			}
			return
		}
		if len(msg.Reply) == 0 {
			return
		}
		rsp := &nats.Msg{
			Subject: msg.Reply,
			Data: data,
		}
		_, msg = c.interceptorChain.applyPub(ctx, msg)
		err = msg.RespondMsg(rsp)
		if err != nil {
			_ = msg.Nak()
			return
		}
	})
	return err
}

func (c *connection) StreamPublish(ctx context.Context, subj string, data []byte) error {
	msg := &nats.Msg{
		Subject: subj,
		Data:    data,
	}
	_, msg = c.interceptorChain.applyPub(ctx, msg)
	_, err := c.js.PublishMsg(msg)
	return err
}

func (c *connection) StreamSubscribe(subj string, reply string, queue string, f func(ctx context.Context, data []byte) ([]byte, error)) error {
	_, err := c.js.QueueSubscribe(subj, queue, func(msg *nats.Msg) {
		ctx := context.Background()
		ctx, msg = c.interceptorChain.applySub(ctx, msg)
		data, err := f(ctx, msg.Data)
		if err != nil {
			if errors.ErrorType(err) == errors.ErrNAK {
				_ = msg.Nak()
			}
			return
		}
		if data == nil || len(reply) == 0 {
			return
		}
		err = c.StreamPublish(ctx, reply, data)
		if err != nil {
			_ = msg.Nak()
			return
		}
	})
	return err
}
