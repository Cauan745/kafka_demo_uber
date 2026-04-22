package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	producerapp "github.com/cauan745/trabalho_kafka/internal/app/producer"
	"github.com/cauan745/trabalho_kafka/internal/kafka/producer"
	"github.com/cauan745/trabalho_kafka/internal/kafka/shared"
)

type Server struct {
	producer *producer.KafkaProducer
	logger   *slog.Logger
}

func NewServer(cfg shared.KafkaConfig) *Server {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	return &Server{
		producer: producer.NewKafkaProducer("", logger, cfg),
		logger:   logger,
	}
}

func main() {
	topic := flag.String("topic", "local_topic", "Kafka Topic Name")
	consumerGroup := flag.String("consumerGroup", "local_cg", "Kafka Consumer Group Name")
	host := flag.String("host", "localhost:9092", "Kafka Host Address ex: 'localhost:9092'")
	driverQuantity := flag.Int("quantity", 1, "The quantity of drivers to simulate")

	flag.Parse()

	config := shared.NewKafkaConfig(*topic, *consumerGroup, *host)

	s := NewServer(*config)

	// Start consumers
	fmt.Println("Starting...")

	producerapp.Start(s.producer, *driverQuantity)
}
