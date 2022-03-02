package redis

import (
	"context"
	"encoding/json"
	"github.com/NICEXAI/go-eventbus"
	"github.com/NICEXAI/go-eventbus/memory"
	"github.com/go-redis/redis/v8"
	"sync"
)

type PubSub struct {
	client *redis.Client
	eb     *memory.Memory
	pmc    sync.Map
}

func (r *PubSub) Subscribe(ctx context.Context, topic string, handle eventbus.SubscribeHandler) (err error) {
	_ = r.getAndInitProc(ctx, topic)
	return r.eb.Subscribe(ctx, topic, handle)
}

func (r *PubSub) Unsubscribe(ctx context.Context, topic string, handle eventbus.SubscribeHandler) (err error) {
	_ = r.eb.Unsubscribe(ctx, topic, handle)
	// 当前 Topic 如果未被订阅，则主动关闭协程
	if r.eb.HandleCount(topic) == 0 {
		r.getAndInitProc(ctx, topic)()
	}
	return nil
}

func (r *PubSub) SubscribeOnce(ctx context.Context, topic string, handle eventbus.SubscribeHandler) (err error) {
	_ = r.getAndInitProc(ctx, topic)
	return r.eb.SubscribeOnce(ctx, topic, handle)
}

func (r *PubSub) Publish(ctx context.Context, topic string, msg *eventbus.Message) (err error) {
	return r.client.Publish(ctx, topic, msg).Err()
}

func (r *PubSub) HandleCount(topic string) int {
	return r.eb.HandleCount(topic)
}

func (r *PubSub) getAndInitProc(ctx context.Context, topic string) context.CancelFunc {
	var proc context.CancelFunc

	originProc, ok := r.pmc.Load(topic)
	if !ok {
		nCtx, cancel := context.WithCancel(ctx)
		go func() {
			var msg eventbus.Message
			sub := r.client.Subscribe(nCtx, topic)

			for {
				select {
				case <-nCtx.Done():
					return
				case msgInfo := <-sub.Channel():
					if err := json.Unmarshal([]byte(msgInfo.Payload), &msg); err == nil {
						_ = r.eb.Publish(ctx, topic, &msg)
					}
				}
			}
		}()
		proc = cancel
		r.pmc.Store(topic, proc)
	} else {
		proc, _ = originProc.(context.CancelFunc)
	}

	return proc
}
