package redis

import (
	"context"
	"fmt"
	"github.com/NICEXAI/go-eventbus"
	"github.com/NICEXAI/go-eventbus/memory"
	"github.com/go-redis/redis/v8"
	"math/rand"
	"sync"
)

// NewEventBusByRedis 初始化基于 Redis 的事件中心
func NewEventBusByRedis(client *redis.Client) (eventbus.EventBus, error) {
	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, err
	}

	if isSupportStream(context.Background(), client) {
		return &Stream{
			id:     fmt.Sprintf("G-%v", rand.Int()),
			client: client,
			eb:     memory.NewEventBusByMemory(),
			pmc:    sync.Map{},
		}, nil
	}

	return &PubSub{
		client: client,
		eb:     memory.NewEventBusByMemory(),
		pmc:    sync.Map{},
	}, nil
}
