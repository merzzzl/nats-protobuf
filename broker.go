package broker

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go"
)

type Options struct {
	Host       string `json:"Host"`
	Port       string `json:"Port"`
	User       string `json:"User"`
	Pass       string `json:"Pass"`
	ClusterId  string `json:"ClusterId"`
}

type jetBroker struct {
	JetStream nats.JetStreamContext
}

type broker struct {
	*publisher
	*subscriber
}

type Broker interface {
	Pub(context.Context, string, []byte) error
	Sub(string, SubFunc) error
}

type SubOptions struct {
	options []SubOption
}

type PubOptions struct {
	options []PubOption
}

type subscriber struct {
	broker  *jetBroker
	options []SubOption
}

type SubOption func(context.Context, *nats.Msg) (context.Context, *nats.Msg)

type SubFunc func(ctx context.Context, subject string, msg []byte) error

type publisher struct {
	broker  *jetBroker
	options []PubOption
}

type PubOption func(context.Context, *nats.Msg) *nats.Msg

func newJetBroker(ops *Options, streamName string) (*jetBroker, error) {
	url := fmt.Sprintf("nats://%s:%s@%s:%s", ops.User, ops.Pass, ops.Host, ops.Port)
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	cfg := &nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{streamName + ".*"},
	}
	_, err = js.AddStream(cfg)
	if err != nil {
		return nil, err
	}
	broker := &jetBroker{
		JetStream: js,
	}
	return broker, nil
}

func NewBroker(ops *Options, streamName string, pubOps *PubOptions, subOps *SubOptions) (Broker, error) {
	jetBr, err := newJetBroker(ops, streamName)
	if err != nil {
		return nil, err
	}
	br := &broker{
		subscriber: newSubscriber(jetBr, subOps.options...),
		publisher: newPublisher(jetBr, pubOps.options...),
	}
	return br, nil
}

func SubOptionsChain(ops ...SubOption) *SubOptions {
	return &SubOptions{options: ops}
}

func PubOptionsChain(ops ...PubOption) *PubOptions {
	return &PubOptions{options: ops}
}

func newSubscriber(broker *jetBroker, options ...SubOption) *subscriber {
	pub := &subscriber{
		broker:  broker,
		options: options,
	}
	return pub
}

func (s *subscriber) Sub(subject string, fn SubFunc) error {
	msgFn := func(msg *nats.Msg) {
		ctx := context.Background()
		for _, option := range s.options {
			ctx, msg = option(ctx, msg)
		}
		err := fn(ctx, msg.Subject, msg.Data)
		if err != nil {
			_ = msg.Nak()
		}
	}
	_, err := s.broker.JetStream.QueueSubscribe(subject, subject+"_queue", msgFn)
	if err != nil {
		return err
	}
	return nil
}

func newPublisher(broker *jetBroker, options ...PubOption) *publisher {
	pub := &publisher{
		broker:  broker,
		options: options,
	}
	return pub
}

func (p *publisher) Pub(ctx context.Context, subject string, data []byte) error {
	msg := &nats.Msg{Data: data}
	for _, option := range p.options {
		msg = option(ctx, msg)
	}
	_, err := p.broker.JetStream.PublishMsg(msg)
	if err != nil {
		return err
	}
	return nil
}
