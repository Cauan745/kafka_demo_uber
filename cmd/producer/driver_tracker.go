package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	consumerapp "github.com/cauan745/trabalho_kafka/internal/app/consumer"
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
	disableConsumer := flag.Bool("no-consumer", false, "Disable the kafka consumer for ride requests")

	flag.Parse()

	config := shared.NewKafkaConfig(*topic, *consumerGroup, *host)

	s := NewServer(*config)

	var consumerCh chan string
	if !*disableConsumer {
		logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
		consumerCh = make(chan string)

		reqConfig := shared.NewKafkaConfig("ride_requests", *consumerGroup, *host)
		err := consumerapp.NewConsumer(consumerCh, logger, *reqConfig)
		if err != nil {
			logger.Error("Failed to initialize consumer", "error", err)
		}
	}

	// Start consumers
	fmt.Println("Starting...")

	producerapp.Start(s.producer, *driverQuantity, consumerCh)

	fmt.Println("Producer finished")
}
