package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/nilorg/eventbus"
)

func main() {
	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	var (
		eventBus eventbus.EventBus
		err      error
	)
	eventBus, err = eventbus.NewRedis(rdb)
	if err != nil {
		panic(err)
	}
	ctx = eventbus.NewGroupIDContext(ctx, "example")
	err = eventBus.SubscribeAsync(ctx, "eventbus.examples.redis", func(ctx context.Context, msg *eventbus.Message) error {
		fmt.Printf("11111-接受的数据：%+v\n", msg)
		return nil
	})
	if err != nil {
		panic(err)
	}
	err = eventBus.SubscribeAsync(ctx, "eventbus.examples.redis", func(ctx context.Context, msg *eventbus.Message) error {
		fmt.Printf("22222-接受的数据：%+v\n", msg)
		return nil
	})
	if err != nil {
		panic(err)
	}
	for i := 0; i < 30; i++ {
		err = eventBus.Publish(ctx, "eventbus.examples.redis", fmt.Sprintf("xxxxxxx index: v%d", i))
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Second * 2)
	}
}
