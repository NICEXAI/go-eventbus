package redis

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/NICEXAI/go-eventbus"
	"github.com/NICEXAI/go-eventbus/memory"
	"github.com/go-redis/redis/v8"
	"log"
	"sync"
)

type message struct {
	Key, Value string
}

type Stream struct {
	id     string
	client *redis.Client
	eb     *memory.Memory
	pmc    sync.Map
}

func (r *Stream) Subscribe(ctx context.Context, topic string, handle eventbus.SubscribeHandler) (err error) {
	_ = r.getAndInitProc(ctx, topic)
	return r.eb.Subscribe(ctx, topic, handle)
}

func (r *Stream) Unsubscribe(ctx context.Context, topic string, handle eventbus.SubscribeHandler) (err error) {
	_ = r.eb.Unsubscribe(ctx, topic, handle)
	// 当前 Topic 如果未被订阅，则主动关闭协程并从Stream中移除消费组
	if r.eb.HandleCount(topic) == 0 {
		r.getAndInitProc(ctx, topic)()
		return r.client.XGroupDestroy(ctx, topic, r.id).Err()
	}
	return nil
}

func (r *Stream) SubscribeOnce(ctx context.Context, topic string, handle eventbus.SubscribeHandler) (err error) {
	_ = r.getAndInitProc(ctx, topic)
	return r.eb.SubscribeOnce(ctx, topic, handle)
}

func (r *Stream) Publish(ctx context.Context, topic string, msg *eventbus.Message) (err error) {
	return r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: topic,
		Values: map[string]interface{}{
			"key":   msg.Key,
			"value": string(msg.Value),
		},
	}).Err()
}

func (r *Stream) HandleCount(topic string) int {
	return r.eb.HandleCount(topic)
}

func (r *Stream) getAndInitProc(ctx context.Context, topic string) context.CancelFunc {
	var proc context.CancelFunc

	originProc, ok := r.pmc.Load(topic)
	if !ok {
		nCtx, cancel := context.WithCancel(ctx)

		go func() {
			var (
				msg        message
				groupState bool
			)

			groups, err := r.client.XInfoGroups(ctx, topic).Result()
			if err != nil {
				if err.Error() != "ERR no such key" {
					log.Printf("%v failed to search info: %v", topic, err)
					return
				}
				if err = r.client.XAdd(ctx, &redis.XAddArgs{
					Stream: topic,
					Values: map[string]interface{}{
						"stream init": topic,
					},
				}).Err(); err != nil {
					log.Printf("%v failed to create stream: %v", topic, err)
					return
				}
			}
			for _, group := range groups {
				if group.Name == r.id {
					groupState = true
					break
				}
			}

			if !groupState {
				if err = r.client.XGroupCreate(ctx, topic, r.id, "$").Err(); err != nil {
					log.Printf("%v failed to create group: %v", topic, err)
					return
				}
			}

			for {
				select {
				case <-nCtx.Done():
					return
				default:
					var streams []redis.XStream

					streams, err = r.client.XReadGroup(nCtx, &redis.XReadGroupArgs{
						Group:    r.id,
						Consumer: "go_event_bus",
						Streams:  []string{topic, ">"},
						Count:    1,
					}).Result()
					if err != nil && !errors.Is(err, context.Canceled) {
						log.Printf("%v failed to run read group: %v", topic, err)
						return
					}
					for _, stream := range streams {
						for _, msgInfo := range stream.Messages {
							bData, _ := json.Marshal(msgInfo.Values)
							_ = json.Unmarshal(bData, &msg)
							_ = r.eb.Publish(nCtx, topic, &eventbus.Message{
								Key:   msg.Key,
								Value: []byte(msg.Value),
							})
						}
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
