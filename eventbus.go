package eventbus

import (
	"context"
	"encoding/json"
)

type Message struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func (m Message) MarshalBinary() (data []byte, err error) {
	return json.Marshal(m)
}

type SubscribeHandler func(msg *Message)

// EventBus event bus
type EventBus interface {
	// Subscribe to a topic
	Subscribe(ctx context.Context, topic string, handle SubscribeHandler) (err error)
	// Unsubscribe to a topic
	Unsubscribe(ctx context.Context, topic string, handle SubscribeHandler) (err error)
	// SubscribeOnce subscribes to a topic once. Handler will be removed after executing
	SubscribeOnce(ctx context.Context, topic string, handle SubscribeHandler) (err error)

	// Publish post a message to the topic
	Publish(ctx context.Context, topic string, msg *Message) (err error)

	// HandleCount get the number of Handle subscriptions for a topic
	HandleCount(topic string) int
}
