package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	gcpconfig "github.com/abtin/pubsubdemo"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"log"
	"time"
)

func main() {
	config, err := gcpconfig.NewGcpConfig()
	if err != nil {
		log.Fatalln(err)
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, config.ProjectID(), option.WithCredentialsFile(config.Credentials()))
	if err != nil {
		log.Fatalf("Error creating a new pubsub client - %s", err)
	}
	defer client.Close()

	topic := client.Topic(config.DataTopicID())
	var alreadySubscribed bool
	for subs := topic.Subscriptions(ctx); ; {
		sub, err := subs.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalln(err)
		}
		if sub.ID() == config.Subscription() {
			alreadySubscribed = true
		}
	}
	var subs *pubsub.Subscription
	if !alreadySubscribed {
		cfg := pubsub.SubscriptionConfig{
			Topic:       client.Topic(config.DataTopicID()),
			AckDeadline: 10 * time.Second,
		}

		subs, err = client.CreateSubscription(ctx, config.Subscription(), cfg)
		if err != nil {
			log.Fatalf("Error creating subscription %s", err)
		}
	} else {
		subs = client.Subscription(config.Subscription())
	}
	defer subs.Delete(ctx)

	log.Println("press ^C to break listening for messages")
	for {
		err = subs.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			log.Printf("Got message: %s", m.Data)
			m.Ack()
		})
		if err != nil {
			log.Fatalf("Error receiving messages - %s", err)
		}
	}
}
