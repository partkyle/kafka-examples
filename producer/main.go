package main

import (
	"flag"
	"log"
	"time"

	"github.com/Shopify/sarama"
)

var (
	kafka   = flag.String("kafka", ":2181", "kafka addr")
	topic   = flag.String("topic", "", "topic to post")
	key     = flag.String("key", "", "key to post message with")
	message = flag.String("message", "", "message to post")
)

func main() {
	flag.Parse()

	clientConfig := &sarama.ClientConfig{MetadataRetries: 10, WaitForElection: 250 * time.Millisecond}
	client, err := sarama.NewClient("sarama", []string{*kafka}, clientConfig)
	if err != nil {
		log.Fatalf("err creating client: %s", err)
	}

	producerConfig := &sarama.ProducerConfig{MaxBufferedBytes: 10 * 1024 * 1024, MaxBufferTime: 1024}
	producer, err := sarama.NewProducer(client, producerConfig)
	if err != nil {
		log.Fatalf("err creating producer: %s", err)
	}

	err = producer.SendMessage(*topic, sarama.StringEncoder(*key), sarama.StringEncoder(*message))
	if err != nil {
		log.Fatalf("err sending message: %s", err)
	}
}
