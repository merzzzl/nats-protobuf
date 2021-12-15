package broker

import (
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
)

type Options struct {
	Host      string `json:"Host"`
	Port      string `json:"Port"`
	User      string `json:"User"`
	Pass      string `json:"Pass"`
	ClusterId string `json:"ClusterId"`
}

type BrokerHandler interface {
	Serve() error
}

type broker struct {
	handlers    []BrokerHandler
	nats        *nats.Conn
	jet         nats.JetStreamContext
	interceptor *InterceptorChain
}

type Broker interface {
	Conn() Connection
	RegisterHandler(BrokerHandler)
	AddStream(streamName string, streamSubjects... string) error
	Serve() error
}

func NewBroker(ops *Options, interceptor *InterceptorChain) (Broker, error) {
	url := fmt.Sprintf("nats://%s:%s@%s:%s", ops.User, ops.Pass, ops.Host, ops.Port)
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	js, err := nc.JetStream()
	if err != nil {
		return nil, err
	}
	br := &broker{
		handlers:    []BrokerHandler{},
		nats:        nc,
		jet:         js,
		interceptor: interceptor,
	}
	return br, nil
}

func (b *broker) Conn() Connection {
	return &connection{
		nats:             b.nats,
		js:               b.jet,
		interceptorChain: b.interceptor,
	}
}

func (b *broker) RegisterHandler(h BrokerHandler) {
	b.handlers = append(b.handlers, h)
}

func (b *broker) AddStream(streamName string, streamSubjects... string) error {
	cfg := &nats.StreamConfig{
		Name:     streamName,
		Subjects: streamSubjects,
	}
	_, err := b.jet.AddStream(cfg)
	if err != nil && strings.Compare(err.Error(), "stream name already in use") == 0 {
		_, err = b.jet.UpdateStream(cfg)
	}
	if err != nil {
		return err
	}
	return nil
}

func (b *broker) Serve() error {
	var wg errgroup.Group
	for _, h := range b.handlers {
		wg.Go(h.Serve)
	}
	return wg.Wait()
}
