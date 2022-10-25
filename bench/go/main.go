package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const N = 1000000
const P = 8

func main() {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "127.0.0.1",
		"compression.codec": "none",
		"linger.ms":         "10000",
	})
	if err != nil {
		panic(err)
	}
	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Error: %v\n", ev.TopicPartition.Error)
				}
			}
		}
	}()

	start := time.Now()
	topic := "bench-publish-1M"
	for i := 0; i < N; i++ {
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: int32(i % P),
			},
			Key:   []byte("k"),
			Value: []byte("v"),
		}, nil)
	}

	p.Flush(60000)
	log.Println(time.Since(start))
}
