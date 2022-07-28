package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"time"

	"github.com/isayme/go-amqp-reconnect/rabbitmq"
	"github.com/nilorg/eventbus"
)

func main() {
	go func() {
		http.ListenAndServe("0.0.0.0:6060", nil)
	}()
	var err error
	var conn *rabbitmq.Connection
	conn, err = rabbitmq.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		panic(err)
	}
	var bus eventbus.EventBus
	ops := eventbus.DefaultRabbitMQOptions
	ops.PoolMinOpen = 1
	ops.PoolMaxOpen = 20 // PoolMaxOpen > subscribe count;
	bus, err = eventbus.NewRabbitMQ(conn, &ops)
	if err != nil {
		panic(err)
	}
	ctx := context.Background()

	topic := "eventbus.examples.rabbitmq.async"
	ctxGroup1 := eventbus.NewGroupIDContext(ctx, "group1")
	err = bus.SubscribeAsync(ctxGroup1, topic, func(ctx context.Context, msg *eventbus.Message) error {
		fmt.Printf("group1-1 %s: %+v\n", topic, msg)
		return nil
	},
	)
	if err != nil {
		panic(err)
	}
	err = bus.SubscribeAsync(ctxGroup1, topic, func(ctx context.Context, msg *eventbus.Message) error {
		fmt.Printf("group1-2 %s: %+v\n", topic, msg)
		return nil
	},
	)
	if err != nil {
		panic(err)
	}
	ctxGroup2 := eventbus.NewGroupIDContext(ctx, "group2")
	err = bus.SubscribeAsync(ctxGroup2, topic,
		func(ctx context.Context, msg *eventbus.Message) error {
			fmt.Printf("group2-1 %s: %+v\n", topic, msg)
			return nil
		},
	)
	if err != nil {
		panic(err)
	}
	err = bus.SubscribeAsync(ctxGroup2, topic,
		func(ctx context.Context, msg *eventbus.Message) error {
			fmt.Printf("group2-2 %s: %+v\n", topic, msg)
			return nil
		},
	)
	if err != nil {
		panic(err)
	}
	// go func() {
	// 	err = bus.Subscribe(ctxGroup2, topic,
	// 		func(ctx context.Context, msg *eventbus.Message) error {
	// 			fmt.Printf("group2-3 %s: %+v\n", topic, msg)
	// 			return nil
	// 		},
	// 	)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }()
	// go func() {
	// 	err = bus.Subscribe(ctxGroup2, topic,
	// 		func(ctx context.Context, msg *eventbus.Message) error {
	// 			fmt.Printf("group2-3 %s: %+v\n", topic, msg)
	// 			return nil
	// 		},
	// 	)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }()
	// go func() {
	// 	err = bus.Subscribe(ctxGroup2, topic,
	// 		func(ctx context.Context, msg *eventbus.Message) error {
	// 			fmt.Printf("group2-3 %s: %+v\n", topic, msg)
	// 			return nil
	// 		},
	// 	)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }()
	// go func() {
	// 	err = bus.Subscribe(ctxGroup2, topic,
	// 		func(ctx context.Context, msg *eventbus.Message) error {
	// 			fmt.Printf("group2-3 %s: %+v\n", topic, msg)
	// 			return nil
	// 		},
	// 	)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }()
	// go func() {
	// 	err = bus.Subscribe(ctxGroup2, topic,
	// 		func(ctx context.Context, msg *eventbus.Message) error {
	// 			fmt.Printf("group2-3 %s: %+v\n", topic, msg)
	// 			return nil
	// 		},
	// 	)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }()
	// go func() {
	// 	err = bus.Subscribe(ctxGroup2, topic,
	// 		func(ctx context.Context, msg *eventbus.Message) error {
	// 			fmt.Printf("group2-3 %s: %+v\n", topic, msg)
	// 			return nil
	// 		},
	// 	)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }()
	// go func() {
	// 	err = bus.Subscribe(ctxGroup2, topic,
	// 		func(ctx context.Context, msg *eventbus.Message) error {
	// 			fmt.Printf("group2-3 %s: %+v\n", topic, msg)
	// 			return nil
	// 		},
	// 	)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }()
	// go func() {
	// 	err = bus.Subscribe(ctxGroup2, topic,
	// 		func(ctx context.Context, msg *eventbus.Message) error {
	// 			fmt.Printf("group2-3 %s: %+v\n", topic, msg)
	// 			return nil
	// 		},
	// 	)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }()
	// go func() {
	// 	err = bus.Subscribe(ctxGroup2, topic,
	// 		func(ctx context.Context, msg *eventbus.Message) error {
	// 			fmt.Printf("group2-3 %s: %+v\n", topic, msg)
	// 			return nil
	// 		},
	// 	)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }()
	// go func() {
	// 	err = bus.Subscribe(ctxGroup2, topic,
	// 		func(ctx context.Context, msg *eventbus.Message) error {
	// 			fmt.Printf("group2-3 %s: %+v\n", topic, msg)
	// 			return nil
	// 		},
	// 	)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }()
	time.Sleep(1 * time.Second)
	for i := 0; i < 10; i++ {
		go func() {
			err = bus.PublishAsync(ctx, topic, fmt.Sprintf("async message: %d", i))
			if err != nil {
				fmt.Printf("publish error: %s\n", err)
			}
			fmt.Println("===============1发送消息结束")
		}()
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(10 * time.Second)
	for i := 0; i < 10; i++ {
		go func() {
			err = bus.PublishAsync(ctx, topic, fmt.Sprintf("async message: %d", i))
			if err != nil {
				fmt.Printf("publish error: %s\n", err)
			}
			fmt.Println("===============2发送消息结束")
		}()
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(120 * time.Second)
	for i := 0; i < 10; i++ {
		go func() {
			err = bus.PublishAsync(ctx, topic, fmt.Sprintf("async message: %d", i))
			if err != nil {
				fmt.Printf("publish error: %s\n", err)
			}
			fmt.Println("===============3发送消息结束")
		}()
		time.Sleep(10 * time.Millisecond)
	}

	for {
		fmt.Printf("PID: %d, GroutineCount: %d, Channel OK\n", os.Getpid(), runtime.NumGoroutine())
		time.Sleep(time.Second)
	}
}
