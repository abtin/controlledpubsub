package main

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/abtin/controlledpubsub/internal/gcp"
	"os"
	"time"
)

func main() {
	config, err := gcp.NewPubSubConfig()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	ctx := context.Background()
	client, err := gcp.NewPubSubClient(ctx, config)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer func() {
		_ = client.Subscription().SeekToTime(ctx, time.Now())
		if err := client.Shutdown(ctx); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()

	fmt.Println("press ^C to break listening for messages")
	for {
		err = client.Subscription().Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			fmt.Printf("message: %s\n", m.Data)
			m.Ack()
		})
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
}
