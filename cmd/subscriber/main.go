package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"google.golang.org/api/option"
	"log"
	"os"
	"time"
)

func main() {
	credentials := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	if credentials == "" {
		log.Fatalln("Please set GOOGLE_APPLICATION_CREDENTIALS environment variable")
	}
	projectID := os.Getenv("GOOGLE_PROJECT_ID")
	if projectID == "" {
		log.Fatalln("Please set GOOGLE_PROJECT_ID environment variable")
	}
	dataTopicID := os.Getenv("DATA_TOPIC_ID")
	if dataTopicID == "" {
		log.Fatalln("Please set DATA_TOPIC_ID environment variable")
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID, option.WithCredentialsFile(credentials))
	if err != nil {
		log.Fatalf("Error creating a new pubsub client - %s", err)
	}
	defer client.Close()

	cfg := pubsub.SubscriptionConfig{
		Topic:       client.Topic(dataTopicID),
		AckDeadline: 10 * time.Second,
	}
	subs, err := client.CreateSubscription(ctx, "test", cfg)
	if err != nil {
		log.Fatalf("Error creating subscription %s", err)
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
