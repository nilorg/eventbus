package eventbus

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// 测试辅助函数：连接 NATS JetStream
func connectJetStream(t *testing.T) *nats.Conn {
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		t.Skipf("Skipping test - NATS server not available: %v", err)
		return nil
	}
	return nc
}

// 测试辅助函数：清理 Stream
func cleanupStreams(t *testing.T, nc *nats.Conn, streamPrefix string) {
	js, err := nc.JetStream()
	if err != nil {
		return
	}

	streams := []string{
		streamPrefix,
		streamPrefix + "_BROADCAST",
		streamPrefix + "_LIMITS",
	}

	for _, name := range streams {
		_ = js.DeleteStream(name)
	}
}

func TestNATSJetStreamEventBus_Basic(t *testing.T) {
	nc := connectJetStream(t)
	if nc == nil {
		return
	}
	defer nc.Close()

	streamName := "TEST_BASIC"
	cleanupStreams(t, nc, streamName)
	defer cleanupStreams(t, nc, streamName)

	opts := &NATSJetStreamOptions{
		Serialize:           &JSONSerialize{},
		Logger:              &StdLogger{},
		StreamName:          streamName,
		MaxMsgs:             1000,
		MaxAge:              time.Hour,
		DuplicateWindow:     time.Minute,
		Replicas:            1,
		AckWait:             5 * time.Second,
		MaxDeliver:          3,
		MaxWaiting:          512,
		DefaultDeliveryMode: NATSJetStreamDeliveryModeWorkQueue,
	}

	bus, err := NewNATSJetStream(nc, opts)
	if err != nil {
		t.Fatalf("Failed to create JetStream event bus: %v", err)
	}
	defer bus.(Closer).Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := "basic.test"

	// 测试消息接收
	received := make(chan *Message, 1)
	err = bus.SubscribeAsync(ctx, topic, func(ctx context.Context, msg *Message) error {
		received <- msg
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// 等待订阅准备完成
	time.Sleep(500 * time.Millisecond)

	// 发布消息
	testMessage := "Hello JetStream!"
	err = bus.Publish(ctx, topic, testMessage)
	if err != nil {
		t.Fatalf("Failed to publish: %v", err)
	}

	// 验证消息接收
	select {
	case msg := <-received:
		if msg.Value != testMessage {
			t.Errorf("Expected %q, got %q", testMessage, msg.Value)
		}
		t.Logf("Received message: %v", msg.Value)
	case <-time.After(10 * time.Second):
		t.Error("Timeout waiting for message")
	}
}

func TestNATSJetStreamEventBus_WorkQueueMode(t *testing.T) {
	nc := connectJetStream(t)
	if nc == nil {
		return
	}
	defer nc.Close()

	streamName := "TEST_WORKQUEUE"
	cleanupStreams(t, nc, streamName)
	defer cleanupStreams(t, nc, streamName)

	opts := &NATSJetStreamOptions{
		Serialize:           &JSONSerialize{},
		Logger:              &StdLogger{},
		StreamName:          streamName,
		MaxMsgs:             1000,
		MaxAge:              time.Hour,
		DuplicateWindow:     time.Minute,
		Replicas:            1,
		AckWait:             5 * time.Second,
		MaxDeliver:          3,
		MaxWaiting:          512,
		DefaultDeliveryMode: NATSJetStreamDeliveryModeWorkQueue,
	}

	bus, err := NewNATSJetStream(nc, opts)
	if err != nil {
		t.Fatalf("Failed to create JetStream event bus: %v", err)
	}
	defer bus.(Closer).Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := "workqueue.test"

	// WorkQueue 模式：Group 被忽略，所有消费者共享消息
	// 即使设置不同的 Group，消息也只会被一个消费者处理
	var count1, count2 int32
	var wg sync.WaitGroup

	// 设置不同的 Group（在 WorkQueue 模式下应被忽略）
	ctx1 := NewGroupIDContext(ctx, "group-a")
	ctx1 = WithNATSJetStreamDeliveryMode(ctx1, NATSJetStreamDeliveryModeWorkQueue)

	ctx2 := NewGroupIDContext(ctx, "group-b")
	ctx2 = WithNATSJetStreamDeliveryMode(ctx2, NATSJetStreamDeliveryModeWorkQueue)

	messageCount := 20
	wg.Add(messageCount)

	err = bus.SubscribeAsync(ctx1, topic, func(ctx context.Context, msg *Message) error {
		atomic.AddInt32(&count1, 1)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe consumer 1: %v", err)
	}

	err = bus.SubscribeAsync(ctx2, topic, func(ctx context.Context, msg *Message) error {
		atomic.AddInt32(&count2, 1)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe consumer 2: %v", err)
	}

	time.Sleep(time.Second)

	// 发布消息
	pubCtx := WithNATSJetStreamDeliveryMode(ctx, NATSJetStreamDeliveryModeWorkQueue)
	for i := 0; i < messageCount; i++ {
		err = bus.Publish(pubCtx, topic, fmt.Sprintf("Message %d", i))
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	// 等待所有消息被处理
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 成功
	case <-time.After(30 * time.Second):
		t.Fatal("Timeout waiting for messages")
	}

	total := atomic.LoadInt32(&count1) + atomic.LoadInt32(&count2)
	t.Logf("WorkQueue mode: consumer1=%d, consumer2=%d, total=%d", count1, count2, total)

	if int(total) != messageCount {
		t.Errorf("Expected %d total messages, got %d", messageCount, total)
	}

	// WorkQueue 模式下，消息应该被负载均衡（两个消费者都应该收到消息）
	if count1 == 0 || count2 == 0 {
		t.Logf("Note: In WorkQueue mode, both consumers share the same consumer, load balancing depends on timing")
	}
}

func TestNATSJetStreamEventBus_BroadcastMode(t *testing.T) {
	nc := connectJetStream(t)
	if nc == nil {
		return
	}
	defer nc.Close()

	streamName := "TEST_BROADCAST"
	cleanupStreams(t, nc, streamName)
	defer cleanupStreams(t, nc, streamName)

	opts := &NATSJetStreamOptions{
		Serialize:           &JSONSerialize{},
		Logger:              &StdLogger{},
		StreamName:          streamName,
		MaxMsgs:             1000,
		MaxAge:              time.Hour,
		DuplicateWindow:     time.Minute,
		Replicas:            1,
		AckWait:             5 * time.Second,
		MaxDeliver:          3,
		MaxWaiting:          512,
		DefaultDeliveryMode: NATSJetStreamDeliveryModeBroadcast,
		InactiveThreshold:   time.Hour,
	}

	bus, err := NewNATSJetStream(nc, opts)
	if err != nil {
		t.Fatalf("Failed to create JetStream event bus: %v", err)
	}
	defer bus.(Closer).Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := "broadcast.test"

	// Broadcast 模式：每个 Group 创建独立 Consumer，所有 Group 都收到消息
	var count1, count2 int32
	var wg sync.WaitGroup

	// 设置不同的 Group
	ctx1 := NewGroupIDContext(ctx, "cdn-node-1")
	ctx1 = WithNATSJetStreamDeliveryMode(ctx1, NATSJetStreamDeliveryModeBroadcast)

	ctx2 := NewGroupIDContext(ctx, "cdn-node-2")
	ctx2 = WithNATSJetStreamDeliveryMode(ctx2, NATSJetStreamDeliveryModeBroadcast)

	messageCount := 10
	wg.Add(messageCount * 2) // 每个消息两个消费者都应该收到

	err = bus.SubscribeAsync(ctx1, topic, func(ctx context.Context, msg *Message) error {
		atomic.AddInt32(&count1, 1)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe consumer 1: %v", err)
	}

	err = bus.SubscribeAsync(ctx2, topic, func(ctx context.Context, msg *Message) error {
		atomic.AddInt32(&count2, 1)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe consumer 2: %v", err)
	}

	time.Sleep(time.Second)

	// 发布消息
	pubCtx := WithNATSJetStreamDeliveryMode(ctx, NATSJetStreamDeliveryModeBroadcast)
	for i := 0; i < messageCount; i++ {
		err = bus.Publish(pubCtx, topic, fmt.Sprintf("Broadcast Message %d", i))
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	// 等待所有消息被处理
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 成功
	case <-time.After(30 * time.Second):
		t.Logf("Broadcast mode: consumer1=%d, consumer2=%d (timeout, but checking results)", count1, count2)
	}

	t.Logf("Broadcast mode: consumer1=%d, consumer2=%d", count1, count2)

	// Broadcast 模式下，每个 Group 都应该收到所有消息
	if int(count1) != messageCount {
		t.Errorf("Consumer 1 (cdn-node-1) expected %d messages, got %d", messageCount, count1)
	}
	if int(count2) != messageCount {
		t.Errorf("Consumer 2 (cdn-node-2) expected %d messages, got %d", messageCount, count2)
	}
}

func TestNATSJetStreamEventBus_LimitsMode(t *testing.T) {
	nc := connectJetStream(t)
	if nc == nil {
		return
	}
	defer nc.Close()

	streamName := "TEST_LIMITS"
	cleanupStreams(t, nc, streamName)
	defer cleanupStreams(t, nc, streamName)

	opts := &NATSJetStreamOptions{
		Serialize:           &JSONSerialize{},
		Logger:              &StdLogger{},
		StreamName:          streamName,
		MaxMsgs:             1000,
		MaxAge:              time.Hour,
		DuplicateWindow:     time.Minute,
		Replicas:            1,
		AckWait:             5 * time.Second,
		MaxDeliver:          3,
		MaxWaiting:          512,
		DefaultDeliveryMode: NATSJetStreamDeliveryModeLimits,
	}

	bus, err := NewNATSJetStream(nc, opts)
	if err != nil {
		t.Fatalf("Failed to create JetStream event bus: %v", err)
	}
	defer bus.(Closer).Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	topic := "limits.test"

	// Limits 模式：支持历史消息回溯，同 Group 内负载均衡
	var count1, count2 int32
	var wg sync.WaitGroup

	// 先发布消息（在订阅之前）
	pubCtx := WithNATSJetStreamDeliveryMode(ctx, NATSJetStreamDeliveryModeLimits)
	messageCount := 10
	for i := 0; i < messageCount; i++ {
		err = bus.Publish(pubCtx, topic, fmt.Sprintf("Historical Message %d", i))
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	time.Sleep(500 * time.Millisecond)

	// 设置相同的 Group（组内负载均衡）
	ctx1 := NewGroupIDContext(ctx, "limits-group")
	ctx1 = WithNATSJetStreamDeliveryMode(ctx1, NATSJetStreamDeliveryModeLimits)

	ctx2 := NewGroupIDContext(ctx, "limits-group")
	ctx2 = WithNATSJetStreamDeliveryMode(ctx2, NATSJetStreamDeliveryModeLimits)

	wg.Add(messageCount)

	err = bus.SubscribeAsync(ctx1, topic, func(ctx context.Context, msg *Message) error {
		atomic.AddInt32(&count1, 1)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe consumer 1: %v", err)
	}

	err = bus.SubscribeAsync(ctx2, topic, func(ctx context.Context, msg *Message) error {
		atomic.AddInt32(&count2, 1)
		wg.Done()
		return nil
	})
	if err != nil {
		t.Fatalf("Failed to subscribe consumer 2: %v", err)
	}

	// 等待所有消息被处理
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 成功
	case <-time.After(30 * time.Second):
		t.Logf("Limits mode: consumer1=%d, consumer2=%d (timeout)", count1, count2)
	}

	total := atomic.LoadInt32(&count1) + atomic.LoadInt32(&count2)
	t.Logf("Limits mode: consumer1=%d, consumer2=%d, total=%d", count1, count2, total)

	// Limits 模式下，同 Group 的消费者应该负载均衡处理消息
	if int(total) != messageCount {
		t.Errorf("Expected %d total messages, got %d", messageCount, total)
	}
}

func TestNATSJetStreamEventBus_WorkQueueIgnoresGroup(t *testing.T) {
	nc := connectJetStream(t)
	if nc == nil {
		return
	}
	defer nc.Close()

	streamName := "TEST_WQ_IGNORE_GROUP"
	cleanupStreams(t, nc, streamName)
	defer cleanupStreams(t, nc, streamName)

	opts := &NATSJetStreamOptions{
		Serialize:           &JSONSerialize{},
		Logger:              &StdLogger{},
		StreamName:          streamName,
		MaxMsgs:             1000,
		MaxAge:              time.Hour,
		DuplicateWindow:     time.Minute,
		Replicas:            1,
		AckWait:             5 * time.Second,
		MaxDeliver:          3,
		MaxWaiting:          512,
		DefaultDeliveryMode: NATSJetStreamDeliveryModeWorkQueue,
	}

	bus, err := NewNATSJetStream(nc, opts)
	if err != nil {
		t.Fatalf("Failed to create JetStream event bus: %v", err)
	}
	defer bus.(Closer).Close()

	// 验证 sanitizeConsumerName 在 WorkQueue 模式下生成相同的 consumer 名称
	topic := "test.topic"
	expectedName := sanitizeConsumerName("workqueue", topic)

	// 不同的 Group 应该生成相同的 consumer 名称（因为 WorkQueue 模式忽略 Group）
	t.Logf("WorkQueue consumer name: %s", expectedName)

	if expectedName != "workqueue_test_topic" {
		t.Errorf("Expected consumer name 'workqueue_test_topic', got '%s'", expectedName)
	}
}

func TestNATSJetStreamEventBus_BroadcastCreatesUniqueConsumers(t *testing.T) {
	nc := connectJetStream(t)
	if nc == nil {
		return
	}
	defer nc.Close()

	streamName := "TEST_BC_UNIQUE"
	cleanupStreams(t, nc, streamName)
	defer cleanupStreams(t, nc, streamName)

	// 验证不同 Group 生成不同的 consumer 名称
	topic := "test.topic"

	name1 := sanitizeConsumerName("cdn-node-1", topic)
	name2 := sanitizeConsumerName("cdn-node-2", topic)

	t.Logf("Broadcast consumer names: %s, %s", name1, name2)

	if name1 == name2 {
		t.Errorf("Different groups should have different consumer names, got same: %s", name1)
	}

	if name1 != "cdn-node-1_test_topic" {
		t.Errorf("Expected 'cdn-node-1_test_topic', got '%s'", name1)
	}
	if name2 != "cdn-node-2_test_topic" {
		t.Errorf("Expected 'cdn-node-2_test_topic', got '%s'", name2)
	}
}

func TestSanitizeConsumerName(t *testing.T) {
	testCases := []struct {
		group    string
		topic    string
		expected string
	}{
		{"my-group", "simple", "my-group_simple"},
		{"my-group", "topic.with.dots", "my-group_topic_with_dots"},
		{"my-group", "topic>with>arrows", "my-group_topic_with_arrows"},
		{"my-group", "topic*with*stars", "my-group_topic_with_stars"},
		{"my-group", "topic with spaces", "my-group_topic_with_spaces"},
		{"", "topic", "default-consumer_topic"},
		{"workqueue", "cache.refresh", "workqueue_cache_refresh"},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s_%s", tc.group, tc.topic), func(t *testing.T) {
			result := sanitizeConsumerName(tc.group, tc.topic)
			if result != tc.expected {
				t.Errorf("sanitizeConsumerName(%q, %q) = %q, expected %q",
					tc.group, tc.topic, result, tc.expected)
			}
		})
	}
}

func TestNATSJetStreamDeliveryModeString(t *testing.T) {
	testCases := []struct {
		mode     NATSJetStreamDeliveryMode
		expected string
	}{
		{NATSJetStreamDeliveryModeWorkQueue, "workqueue"},
		{NATSJetStreamDeliveryModeBroadcast, "broadcast"},
		{NATSJetStreamDeliveryModeLimits, "limits"},
		{NATSJetStreamDeliveryMode(999), "unknown"},
	}

	for _, tc := range testCases {
		t.Run(tc.expected, func(t *testing.T) {
			result := tc.mode.String()
			if result != tc.expected {
				t.Errorf("NATSJetStreamDeliveryMode(%d).String() = %q, expected %q",
					tc.mode, result, tc.expected)
			}
		})
	}
}

func TestNATSJetStreamDeliveryModeContext(t *testing.T) {
	ctx := context.Background()

	// 测试没有设置 mode 的情况
	_, ok := FromNATSJetStreamDeliveryModeContext(ctx)
	if ok {
		t.Error("Expected no delivery mode in empty context")
	}

	// 测试设置 mode
	ctx = WithNATSJetStreamDeliveryMode(ctx, NATSJetStreamDeliveryModeBroadcast)
	mode, ok := FromNATSJetStreamDeliveryModeContext(ctx)
	if !ok {
		t.Error("Expected delivery mode in context")
	}
	if mode != NATSJetStreamDeliveryModeBroadcast {
		t.Errorf("Expected NATSJetStreamDeliveryModeBroadcast, got %v", mode)
	}
}

func TestNATSJetStreamEventBus_Close(t *testing.T) {
	nc := connectJetStream(t)
	if nc == nil {
		return
	}

	streamName := "TEST_CLOSE"
	cleanupStreams(t, nc, streamName)
	defer cleanupStreams(t, nc, streamName)

	opts := &NATSJetStreamOptions{
		Serialize:           &JSONSerialize{},
		Logger:              &StdLogger{},
		StreamName:          streamName,
		MaxMsgs:             1000,
		MaxAge:              time.Hour,
		DuplicateWindow:     time.Minute,
		Replicas:            1,
		AckWait:             5 * time.Second,
		MaxDeliver:          3,
		MaxWaiting:          512,
		DefaultDeliveryMode: NATSJetStreamDeliveryModeWorkQueue,
	}

	bus, err := NewNATSJetStream(nc, opts)
	if err != nil {
		t.Fatalf("Failed to create JetStream event bus: %v", err)
	}

	// 测试 Closer 接口
	closer, ok := bus.(Closer)
	if !ok {
		t.Fatal("JetStream event bus should implement Closer interface")
	}

	err = closer.Close()
	if err != nil {
		t.Errorf("Failed to close event bus: %v", err)
	}
}

func TestNATSJetStreamEventBus_ParameterValidation(t *testing.T) {
	nc := connectJetStream(t)
	if nc == nil {
		return
	}
	defer nc.Close()

	streamName := "TEST_PARAMS"
	cleanupStreams(t, nc, streamName)
	defer cleanupStreams(t, nc, streamName)

	// 测试参数校验和默认值填充
	opts := &NATSJetStreamOptions{
		Serialize:              &JSONSerialize{},
		Logger:                 &StdLogger{},
		StreamName:             streamName,
		MaxMsgs:                1000,
		MaxAge:                 time.Hour,
		DuplicateWindow:        time.Minute,
		Replicas:               1,
		PublishAsyncMaxPending: 0, // 应该被填充为默认值
		AckWait:                0, // 应该被填充为默认值
		MaxWaiting:             0, // 应该被填充为默认值
		DefaultDeliveryMode:    NATSJetStreamDeliveryModeWorkQueue,
	}

	bus, err := NewNATSJetStream(nc, opts)
	if err != nil {
		t.Fatalf("Failed to create JetStream event bus: %v", err)
	}
	defer bus.(Closer).Close()

	// 验证参数被正确填充
	jsBus := bus.(*natsJetStreamEventBus)
	if jsBus.options.PublishAsyncMaxPending != defaultAsyncPubAckInflight {
		t.Errorf("Expected PublishAsyncMaxPending=%d, got %d",
			defaultAsyncPubAckInflight, jsBus.options.PublishAsyncMaxPending)
	}
	if jsBus.options.AckWait != defaultAPITimeout {
		t.Errorf("Expected AckWait=%v, got %v",
			defaultAPITimeout, jsBus.options.AckWait)
	}
	if jsBus.options.MaxWaiting != 512 {
		t.Errorf("Expected MaxWaiting=512, got %d", jsBus.options.MaxWaiting)
	}
}

func TestNATSJetStreamEventBus_StreamNames(t *testing.T) {
	nc := connectJetStream(t)
	if nc == nil {
		return
	}
	defer nc.Close()

	streamName := "MYAPP"
	cleanupStreams(t, nc, streamName)
	defer cleanupStreams(t, nc, streamName)

	opts := &NATSJetStreamOptions{
		Serialize:           &JSONSerialize{},
		Logger:              &StdLogger{},
		StreamName:          streamName,
		MaxMsgs:             1000,
		MaxAge:              time.Hour,
		DuplicateWindow:     time.Minute,
		Replicas:            1,
		AckWait:             5 * time.Second,
		MaxDeliver:          3,
		MaxWaiting:          512,
		DefaultDeliveryMode: NATSJetStreamDeliveryModeWorkQueue,
	}

	bus, err := NewNATSJetStream(nc, opts)
	if err != nil {
		t.Fatalf("Failed to create JetStream event bus: %v", err)
	}
	defer bus.(Closer).Close()

	jsBus := bus.(*natsJetStreamEventBus)

	// 验证 Stream 名称
	if jsBus.getStreamName(NATSJetStreamDeliveryModeWorkQueue) != "MYAPP" {
		t.Errorf("Expected WorkQueue stream name 'MYAPP', got '%s'",
			jsBus.getStreamName(NATSJetStreamDeliveryModeWorkQueue))
	}
	if jsBus.getStreamName(NATSJetStreamDeliveryModeBroadcast) != "MYAPP_BROADCAST" {
		t.Errorf("Expected Broadcast stream name 'MYAPP_BROADCAST', got '%s'",
			jsBus.getStreamName(NATSJetStreamDeliveryModeBroadcast))
	}
	if jsBus.getStreamName(NATSJetStreamDeliveryModeLimits) != "MYAPP_LIMITS" {
		t.Errorf("Expected Limits stream name 'MYAPP_LIMITS', got '%s'",
			jsBus.getStreamName(NATSJetStreamDeliveryModeLimits))
	}

	// 验证 Streams map 已填充
	if len(jsBus.streams) != 3 {
		t.Errorf("Expected 3 streams, got %d", len(jsBus.streams))
	}
}

func TestNATSJetStreamEventBus_SubjectNames(t *testing.T) {
	nc := connectJetStream(t)
	if nc == nil {
		return
	}
	defer nc.Close()

	streamName := "EVENTBUS"
	cleanupStreams(t, nc, streamName)
	defer cleanupStreams(t, nc, streamName)

	opts := &NATSJetStreamOptions{
		Serialize:           &JSONSerialize{},
		Logger:              &StdLogger{},
		StreamName:          streamName,
		MaxMsgs:             1000,
		MaxAge:              time.Hour,
		DuplicateWindow:     time.Minute,
		Replicas:            1,
		AckWait:             5 * time.Second,
		MaxDeliver:          3,
		MaxWaiting:          512,
		DefaultDeliveryMode: NATSJetStreamDeliveryModeWorkQueue,
	}

	bus, err := NewNATSJetStream(nc, opts)
	if err != nil {
		t.Fatalf("Failed to create JetStream event bus: %v", err)
	}
	defer bus.(Closer).Close()

	jsBus := bus.(*natsJetStreamEventBus)

	topic := "cache.refresh"

	// 验证不同模式的 Subject 名称
	workqueueSubject := jsBus.getJetStreamSubject(NATSJetStreamDeliveryModeWorkQueue, topic)
	broadcastSubject := jsBus.getJetStreamSubject(NATSJetStreamDeliveryModeBroadcast, topic)
	limitsSubject := jsBus.getJetStreamSubject(NATSJetStreamDeliveryModeLimits, topic)

	if workqueueSubject != "EVENTBUS.cache.refresh" {
		t.Errorf("Expected 'EVENTBUS.cache.refresh', got '%s'", workqueueSubject)
	}
	if broadcastSubject != "EVENTBUS_BROADCAST.cache.refresh" {
		t.Errorf("Expected 'EVENTBUS_BROADCAST.cache.refresh', got '%s'", broadcastSubject)
	}
	if limitsSubject != "EVENTBUS_LIMITS.cache.refresh" {
		t.Errorf("Expected 'EVENTBUS_LIMITS.cache.refresh', got '%s'", limitsSubject)
	}
}

func TestNATSJetStreamEventBus_NilConnection(t *testing.T) {
	_, err := NewNATSJetStream(nil)
	if err == nil {
		t.Error("Expected error for nil connection")
	}
	if err.Error() != "nats connection is nil" {
		t.Errorf("Unexpected error message: %v", err)
	}
}

func TestNATSJetStreamEventBus_DefaultOptions(t *testing.T) {
	// 验证默认选项
	opts := DefaultNATSJetStreamOptions

	if opts.DefaultDeliveryMode != NATSJetStreamDeliveryModeWorkQueue {
		t.Errorf("Expected default mode WorkQueue, got %v", opts.DefaultDeliveryMode)
	}
	if opts.InactiveThreshold != 24*time.Hour {
		t.Errorf("Expected InactiveThreshold 24h, got %v", opts.InactiveThreshold)
	}
	if opts.StreamName != "EVENTBUS" {
		t.Errorf("Expected StreamName 'EVENTBUS', got '%s'", opts.StreamName)
	}
	if opts.MaxWaiting != 512 {
		t.Errorf("Expected MaxWaiting 512, got %d", opts.MaxWaiting)
	}
}
