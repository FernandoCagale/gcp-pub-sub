package worker

import (
	"cloud.google.com/go/pubsub"
	"context"
	"errors"
	"google.golang.org/api/option"
	"log"
	"os"
	"sync"
	"time"
)

type Worker struct {
	client *pubsub.Client
}

func NewWorker() *Worker {
	client, err := pubsub.NewClient(context.Background(), os.Getenv("PROJECT"), option.WithCredentialsFile(os.Getenv("KEY")))
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
	}

	return &Worker{
		client: client,
	}
}

func (w *Worker) CreateTopic(name string) (*pubsub.Topic, error) {
	topic, err := w.client.CreateTopic(context.Background(), name)
	if err != nil {
		return nil, err
	}

	log.Println("Topic created: ", name)

	return topic, nil
}

func (w *Worker) GetTopic(name string) (*pubsub.Topic, error) {
	topic := w.client.Topic(name)
	ok, err := topic.Exists(context.Background())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("Not found")
	}
	return topic, nil
}

func (w *Worker) CreateSubscription(topic *pubsub.Topic, name string) error {
	_, err := w.client.CreateSubscription(context.Background(), name, pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: 20 * time.Second,
	})
	if err != nil {
		return err
	}

	log.Println("Subscription created: ", name)

	return nil
}

func (w *Worker) Publish(topic *pubsub.Topic, message string) error {
	ctx := context.Background()
	result := topic.Publish(ctx, &pubsub.Message{
		Data: []byte(message),
	})

	serverID, err := result.Get(ctx)
	if err != nil {
		return err
	}
	log.Println("Publish :", serverID)
	return nil
}

func (w *Worker) PullMsgs(topic *pubsub.Topic, name string) error {
	ctx := context.Background()

	var mu sync.Mutex
	received := 0
	sub := w.client.Subscription(name)
	cctx, cancel := context.WithCancel(ctx)
	err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		msg.Ack()
		log.Printf("Subscription %s Got message: %q\n", name, string(msg.Data))
		mu.Lock()
		defer mu.Unlock()

		received++

		if received == 10 {
			cancel()
		}
	})
	if err != nil {
		return err
	}
	return nil
}


