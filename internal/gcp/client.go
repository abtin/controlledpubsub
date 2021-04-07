package gcp

import (
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"time"
)

type PubSubClient struct {
	client *pubsub.Client
	topic  *pubsub.Topic
	sub    *pubsub.Subscription
}

// NewPubSubClient creates a new client instance and if required creates Topic and subscription
func NewPubSubClient(ctx context.Context, config PubSubConfig) (PubSubClient, error) {
	client, err := pubsub.NewClient(ctx, config.ProjectID(), option.WithCredentialsFile(config.Credentials()))
	if err != nil {
		return PubSubClient{}, err
	}
	var dataTopicExist bool
	it := client.Topics(ctx)
	for {
		topic, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Println(err)
		}
		if topic.ID() == config.DataTopicID() {
			dataTopicExist = true
		}
	}
	var dataTopic *pubsub.Topic
	if !dataTopicExist {
		dataTopic, err = client.CreateTopic(ctx, config.DataTopicID())
		if err != nil {
			return PubSubClient{}, err
		}
	} else {
		dataTopic = client.Topic(config.DataTopicID())
		//defer dataTopic.Stop()
	}
	var alreadySubscribed bool
	for subs := dataTopic.Subscriptions(ctx); ; {
		sub, err := subs.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return PubSubClient{}, err
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
			return PubSubClient{}, err
		}
	} else {
		subs = client.Subscription(config.Subscription())
	}

	return PubSubClient{
		client: client,
		topic:  dataTopic,
		sub:    subs,
	}, nil
}

func (ps PubSubClient) Shutdown(ctx context.Context) error {
	if err := ps.sub.Delete(ctx); err != nil {
		return err
	}
	ps.topic.Stop()
	if err := ps.client.Close(); err != nil {
		return err
	}
	return nil
}

func (ps PubSubClient) Client() *pubsub.Client {
	return ps.client
}

func (ps PubSubClient) Topic() *pubsub.Topic {
	return ps.topic
}

func (ps PubSubClient) Subscription() *pubsub.Subscription {
	return ps.sub
}
