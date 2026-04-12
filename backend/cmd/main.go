package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	areatracker "github.com/cauan745/trabalho_kafka/internal/app/consumer/area_tracker"
	driverlogger "github.com/cauan745/trabalho_kafka/internal/app/consumer/driver_logger"
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

func (s *Server) produceMsg() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	id := 0
	for t := range ticker.C {
		msg := fmt.Sprintf("hello from kafka, msgID = %d, ts = %s", id, t.Format("15:20:20"))
		s.producer.Produce(msg)
		id++
	}
}

func main() {
	topic := flag.String("topic", "local_topic", "Kafka Topic Name")
	consumerGroup := flag.String("consumerGroup", "local_cg", "Kafka Consumer Group Name")
	host := flag.String("host", "localhost:9092", "Kafka Host Address ex: 'localhost:9092'")

	flag.Parse()

	config := shared.NewKafkaConfig(*topic, *consumerGroup, *host)

	s := NewServer(*config)

	// Start consumers
	fmt.Println("Starting...")
	go driverlogger.Start(s.logger, *config)

	// only consumers from different groups can consume the same partition
	config.ConsumerGroup = "local_cg2"
	go areatracker.Start(s.logger, areatracker.Area{Long: 4.3, Lat: 3.6}, *config)

	producerapp.Start(s.producer)
}
