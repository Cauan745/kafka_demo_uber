// Package producer
package producer

import (
	"fmt"
	"log/slog"

	"github.com/cauan745/trabalho_kafka/internal/kafka/shared"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type KafkaProducer struct {
	producer *kafka.Producer
	topic    string
	logger   *slog.Logger
}

func NewKafkaProducer(topic string, logger *slog.Logger, cfg shared.KafkaConfig) *KafkaProducer {
	if topic == "" {
		topic = cfg.Topic
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": cfg.Host})
	if err != nil {
		panic(err)
	}

	// defer p.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	return &KafkaProducer{
		p,
		topic,
		logger,
	}
}

func (p *KafkaProducer) Produce(msg string) {
	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
	}, nil)
	if err != nil {
		fmt.Println("Erro:", err)
	}
}
