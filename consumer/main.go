package main

import (
	"flag"
	"log"
	"os"
	"sync"

	"github.com/partkyle/kafka-examples/flagutil"

	"github.com/Shopify/sarama"
)

var (
	logger = log.New(os.Stdout, "[kafka-consumer] ", log.LstdFlags)

	kafkas     flagutil.MultiString = flagutil.MultiString{":2181"}
	topic      string
	partitions flagutil.MultiInt32
	offset     int64
)

func init() {
	flag.Var(&kafkas, "kafkas", "kafka addresses to connect to")
	flag.StringVar(&topic, "topic", "default", "topic to read")
	flag.Var(&partitions, "partitions", "partitions to read from")
	flag.Int64Var(&offset, "offset", 0, "offset to start reading from")

	flag.Parse()
}

func main() {
	logger.Printf("using kafka addresses: %v", kafkas)
	client, err := sarama.NewClient("sarama", []string(kafkas), nil)
	if err != nil {
		logger.Fatalf("err creating client: %s", err)
	}

	if partitions == nil {
		logger.Println("no partitions received - using all partitions")

		var err error
		partitions, err = client.Partitions(topic)
		if err != nil {
			logger.Fatal(err)
		}
	}

	logger.Printf("using partitions %v", partitions)

	var wg sync.WaitGroup
	wg.Add(len(partitions))

	for _, partition := range partitions {
		p := partition
		go func() {
			defer wg.Done()

			consumerConfig := sarama.NewConsumerConfig()
			consumerConfig.OffsetMethod = sarama.OffsetMethodManual
			consumerConfig.OffsetValue = offset

			consumer, err := sarama.NewConsumer(client, topic, p, "", consumerConfig)
			if err != nil {
				logger.Fatalf("err creating consumer: %s", err)
			}
			defer consumer.Close()

			for event := range consumer.Events() {
				logger.Printf("partition=%d offset=%d key=%s\n\tvalue=%s", event.Partition, event.Offset, event.Key, event.Value)
			}
		}()
	}

	wg.Wait()
	logger.Println("exiting normally")
}
