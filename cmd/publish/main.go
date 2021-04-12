package main

import (
	"bufio"
	"cloud.google.com/go/pubsub"
	"context"
	"fmt"
	"github.com/abtin/controlledpubsub/internal/gcp"
	"os"
	"strings"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: ./publish <file-to-publish>")
		os.Exit(1)
	}
	publishFile := os.Args[1]

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
		if err := client.Shutdown(ctx); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}()

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
				fmt.Printf("Error reading input - %s\n", err)
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
		fmt.Printf("cannot open file %q to publish\n", publishFile)
		os.Exit(1)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		if !paused {
			line := scanner.Text()
			pubRes := client.Topic().Publish(ctx, &pubsub.Message{Data: []byte(line)})
			_, err = pubRes.Get(ctx)
			if err != nil {
				fmt.Printf("Error publishing message - %s\n", err)
			}
			time.Sleep(1 * time.Second)
		} else {
			paused = <-ch
		}
	}
}
