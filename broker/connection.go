package broker

import (
	"context"
	"time"

	"github.com/nats-io/nats.go"
)

type Connection interface {
	Publish(ctx context.Context, subj string, data []byte, timeout time.Duration) ([]byte, error)
	Subscribe(subj string, queue string, f func(ctx context.Context, data []byte) ([]byte, error)) error
	StreamPublish(ctx context.Context, subj string, data []byte) error
	StreamSubscribe(subj string, queue string, f func(ctx context.Context, data []byte) error) error
}

type connection struct {
	interceptorChain *InterceptorChain
	nats             *nats.Conn
	js               nats.JetStreamContext
}

func (c *connection) publishMsg(ctx context.Context, subj string, data []byte) *nats.Msg {
	msg := &nats.Msg{
		Subject: subj,
		Data:    data,
	}
	_, msg = c.interceptorChain.applyPub(ctx, msg)
	return msg
}

func (c *connection) handleMsg(msg *nats.Msg) (context.Context, *nats.Msg) {
	ctx := context.Background()
	return c.interceptorChain.applySub(ctx, msg)
}

func (c *connection) Publish(ctx context.Context, subj string, data []byte, timeout time.Duration) ([]byte, error) {
	msg := c.publishMsg(ctx, subj, data)
	msg, err := c.nats.RequestMsg(msg, timeout)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}

func (c *connection) Subscribe(subj string, queue string, f func(ctx context.Context, data []byte) ([]byte, error)) error {
	_, err := c.nats.QueueSubscribe(subj, queue, func(msg *nats.Msg) {
		ctx, msg := c.handleMsg(msg)
		data, err := f(ctx, msg.Data)
		if err != nil {
			_ = msg.Nak()
			return
		}
		msg = c.publishMsg(ctx, msg.Reply, data)
		err = c.nats.PublishMsg(msg)
		if err != nil {
			_ = msg.Nak()
			return
		}
	})
	return err
}

func (c *connection) StreamPublish(ctx context.Context, subj string, data []byte) error {
	msg := c.publishMsg(ctx, subj, data)
	_, err := c.js.PublishMsg(msg)
	if err != nil {
		return err
	}
	return nil
}

func (c *connection) StreamSubscribe(subj string, queue string, f func(ctx context.Context, data []byte) (error)) error {
	_, err := c.js.QueueSubscribe(subj, queue, func(msg *nats.Msg) {
		ctx, msg := c.handleMsg(msg)
		err := f(ctx, msg.Data)
		if err != nil {
			_ = msg.Nak()
			return
		}
	})
	return err
}
