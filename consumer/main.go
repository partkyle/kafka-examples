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

	kafkas      flagutil.MultiString = flagutil.MultiString{":2181"}
	topic       string
	partitions  flagutil.MultiInt32
	offset      int64
	showOffsets bool
)

func init() {
	flag.Var(&kafkas, "kafkas", "kafka addresses to connect to")
	flag.StringVar(&topic, "topic", "", "topic to read")
	flag.Var(&partitions, "partitions", "partitions to read from")
	flag.Int64Var(&offset, "offset", 0, "offset to start reading from")
	flag.BoolVar(&showOffsets, "show-offsets", false, "show offset and exit")

	flag.Parse()
}

func main() {
	logger.Printf("using kafka addresses: %v", kafkas)
	client, err := sarama.NewClient("sarama", []string(kafkas), nil)
	if err != nil {
		logger.Fatalf("err creating client: %s", err)
	}

	if partitions == nil {
		var err error
		partitions, err = client.Partitions(topic)
		if err != nil {
			logger.Fatal(err)
		}
	}

	logger.Printf("using partitions %v", partitions)

	if showOffsets {
		readOffsets(client, topic, partitions)
	} else {
		tailPartitions(client, topic, partitions)
	}
}

func readOffsets(client *sarama.Client, topic string, partitions []int32) {
	for _, partition := range partitions {
		earliest, err := client.GetOffset(topic, partition, sarama.EarliestOffset)
		if err != nil {
			logger.Fatalf("error getting earliest offset for %s:%d: %s", topic, partition, err)
		}

		latest, err := client.GetOffset(topic, partition, sarama.LatestOffsets)
		if err != nil {
			logger.Fatalf("error getting latest offset for %s:%d: %s", topic, partition, err)
		}

		logger.Printf("topic=%s partition=%d earliest=%d latest=%d", topic, partition, earliest, latest)
	}
}

func tailPartitions(client *sarama.Client, topic string, partitions []int32) {
	var wg sync.WaitGroup
	wg.Add(len(partitions))

	tailConsumer := func(partition int32) {
		defer wg.Done()

		consumerConfig := sarama.NewConsumerConfig()
		consumerConfig.OffsetMethod = sarama.OffsetMethodManual
		consumerConfig.OffsetValue = offset

		consumer, err := sarama.NewConsumer(client, topic, partition, "", consumerConfig)
		if err != nil {
			logger.Fatalf("err creating consumer: %s", err)
		}
		defer consumer.Close()

		for event := range consumer.Events() {
			logger.Printf("partition=%d offset=%d key=%s value=%s", event.Partition, event.Offset, event.Key, event.Value)
		}
	}

	for _, partition := range partitions {
		go tailConsumer(partition)
	}

	wg.Wait()
}
