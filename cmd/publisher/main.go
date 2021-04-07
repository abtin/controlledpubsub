package main

import (
	"bufio"
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/abtin/pubsubdemo"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"log"
	"os"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalln("Usage: ./published <publishing_file>")
	}
	publishFile := os.Args[1]
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

	var dataTopicExist bool
	it := client.Topics(ctx)
	for {
		topic, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil { // TODO: Handle error.
			log.Fatalln(err)
		}
		if topic.ID() == config.DataTopicID() {
			dataTopicExist = true
		}
	}
	var dataTopic *pubsub.Topic
	if !dataTopicExist {
		dataTopic, err = client.CreateTopic(ctx, config.DataTopicID())
		if err != nil {
			log.Fatalf("Error creatng topic - %s", err)
		}
	} else {
		dataTopic = client.Topic(config.DataTopicID())
		defer dataTopic.Stop()
	}

	var paused bool
	ch := make(chan bool)
	go func(p chan bool) {
		for {
			var command string
			fmt.Print("command [pause|resume]: ")
			l, err := fmt.Scanf("%s", &command)
			if l == 0 {
				continue
			}
			if err != nil {
				log.Fatalf("Error reading input - %s", err)
			}
			switch strings.ToLower(command) {
			case "pause":
				paused = true
			case "resume":
				paused = false
				p <- false
			}
		}
	}(ch)

	file, err := os.Open(publishFile)
	if err != nil {
		log.Fatalf("cannot open file %q to publish\n", publishFile)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		if !paused {
			line := scanner.Text()
			pubRes := dataTopic.Publish(ctx, &pubsub.Message{Data: []byte(line)})
			_, err = pubRes.Get(ctx)
			if err != nil {
				log.Fatalf("Error publishing message - %s", err)
			}
			time.Sleep(1 * time.Second)
		} else {
			paused = <-ch
		}
	}
}