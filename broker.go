package broker

import (
	"context"
	"crypto/md5"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
)

type Options struct {
	Host      string `json:"Host"`
	Port      string `json:"Port"`
	User      string `json:"User"`
	Pass      string `json:"Pass"`
	ClusterId string `json:"ClusterId"`
}

type jetBroker struct {
	stream   string
	JetStream nats.JetStreamContext
}

type broker struct {
	handlers BrokerHandler
	jb       *jetBroker
	pub      []PubOption
	sub      []SubOption
}

type Broker interface {
	Pub(context.Context, string, []byte) error
	Sub(string, SubFunc) error
	AddStream(string) error
	RegisterHandler(BrokerHandler)
	Serve() error
}

type SubOptions []SubOption

type PubOptions []PubOption

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

type BrokerHandler interface {
	Serve() error
}

func newJetBroker(ops *Options) (*jetBroker, error) {
	url := fmt.Sprintf("nats://%s:%s@%s:%s", ops.User, ops.Pass, ops.Host, ops.Port)
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	broker := &jetBroker{
		JetStream: js,
	}
	return broker, nil
}

func (jb *jetBroker) addStream(streamName string) error {
	cfg := &nats.StreamConfig{
		Name:     streamName,
		Subjects: []string{streamName + ".>"},
	}
	_, err := jb.JetStream.AddStream(cfg)
	if err != nil && strings.Compare(err.Error(), "stream name already in use") == 0 {
		_, err = jb.JetStream.UpdateStream(cfg)
	}
	if err != nil {
		return err
	}
	jb.stream = streamName
	return nil
}

func NewBroker(ops *Options, pubOps PubOptions, subOps SubOptions) (Broker, error) {
	jetBr, err := newJetBroker(ops)
	if err != nil {
		return nil, err
	}
	br := &broker{
		jb:  jetBr,
		sub: subOps,
		pub: pubOps,
	}
	return br, nil
}

func SubOptionsChain(ops ...SubOption) SubOptions {
	return ops
}

func PubOptionsChain(ops ...PubOption) PubOptions {
	return ops
}

func newSubscriber(broker *jetBroker, options ...SubOption) *subscriber {
	pub := &subscriber{
		broker:  broker,
		options: options,
	}
	return pub
}

func (b *broker) RegisterHandler(h BrokerHandler) {
	b.handlers = h
}

func (b *broker) AddStream(streamName string) error {
	return b.jb.addStream(streamName)
}

func (b *broker) Serve() error {
	return b.handlers.Serve()
}

func (b *broker) Sub(subject string, fn SubFunc) error {
	s := newSubscriber(b.jb, b.sub...)
	return s.sub(subject, fn)
}

func (b *broker) Pub(ctx context.Context, subject string, data []byte) error {
	s := newPublisher(b.jb, b.pub...)
	return s.pub(ctx, subject, data)
}

func (s *subscriber) sub(subject string, fn SubFunc) error {
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
	_, err := s.broker.JetStream.QueueSubscribe(subject, subQueue(subject), msgFn)
	if err != nil {
		return err
	}
	return nil
}

func subQueue(subject string) (string) {
	md5Key := md5.Sum([]byte(subject))
	key := fmt.Sprintf("queue_%x", md5Key[12:])
	return key
}

func newPublisher(broker *jetBroker, options ...PubOption) *publisher {
	pub := &publisher{
		broker:  broker,
		options: options,
	}
	return pub
}

func (p *publisher) pub(ctx context.Context, subject string, data []byte) error {
	msg := &nats.Msg{
		Header: nats.Header{},
		Data: data,
		Subject: subject,
	}
	for _, option := range p.options {
		msg = option(ctx, msg)
	}
	_, err := p.broker.JetStream.PublishMsg(msg)
	if err != nil {
		return err
	}
	return nil
}
