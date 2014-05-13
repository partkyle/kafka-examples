package main

import (
	"flag"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

var (
	kafka = flag.String("kafka", ":2181", "kafka addr")
	topic = flag.String("topic", "default", "topic to post")
)

func main() {
	flag.Parse()

	clientConfig := &sarama.ClientConfig{MetadataRetries: 10, WaitForElection: 250 * time.Millisecond}
	client, err := sarama.NewClient("sarama", []string{*kafka}, clientConfig)
	if err != nil {
		log.Fatalf("err creating client: %s", err)
	}

	consumerConfig := &sarama.ConsumerConfig{MaxWaitTime: 1024}
	consumer, err := sarama.NewConsumer(client, *topic, 0, "", consumerConfig)
	if err != nil {
		log.Fatalf("err creating consumer: %s", err)
	}

	for event := range consumer.Events() {
		log.Printf("%s: %s", event.Key, event.Value)
	}
}
