package main

import (
	"context"
	"github.com/NICEXAI/go-eventbus"
	RedisEventBus "github.com/NICEXAI/go-eventbus/redis"
	"github.com/go-redis/redis/v8"
	"log"
	"time"
)

func main() {
	ctx := context.Background()
	topicName := "socket.io"

	redisClient := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:6379",
	})

	eventBus, _ := RedisEventBus.NewEventBusByRedis(redisClient)

	handle := func(msg *eventbus.Message) {
		log.Printf("Subscribe, key: %s, value: %s \n", msg.Key, string(msg.Value))
	}

	if err := eventBus.Subscribe(ctx, topicName, handle); err != nil {
		log.Printf("Subscribe failed：%v \n", err)
		return
	}

	go func() {
		time.Sleep(10 * time.Second)
		if err := eventBus.Unsubscribe(ctx, topicName, handle); err != nil {
			log.Printf("Unsubscribe failed：%v \n", err)
			return
		}
	}()

	if err := eventBus.SubscribeOnce(ctx, topicName, func(msg *eventbus.Message) {
		log.Printf("SubscribeOnce, key: %s, value: %s \n", msg.Key, string(msg.Value))
	}); err != nil {
		log.Printf("SubscribeOnce failed：%v \n", err)
		return
	}

	for {
		if eventBus.HandleCount(topicName) == 0 {
			log.Println("EventBus handle number is 0, exit the program")
			return
		}

		time.Sleep(2 * time.Second)

		if pErr := eventBus.Publish(ctx, topicName, &eventbus.Message{
			Key:   "hello",
			Value: []byte("world"),
		}); pErr != nil {
			log.Printf("Publish failed：%v \n", pErr)
			return
		}
	}
}
