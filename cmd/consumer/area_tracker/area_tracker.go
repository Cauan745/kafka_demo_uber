package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	areatracker "github.com/cauan745/trabalho_kafka/internal/app/consumer/area_tracker"
	"github.com/cauan745/trabalho_kafka/internal/kafka/shared"
)

func main() {
	topic := flag.String("topic", "local_topic", "Kafka Topic Name")
	consumerGroup := flag.String("consumerGroup", "local_cg", "Kafka Consumer Group Name")
	host := flag.String("host", "localhost:9092", "Kafka Host Address ex: 'localhost:9092'")

	flag.Parse()

	config := shared.NewKafkaConfig(*topic, *consumerGroup, *host)

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Start consumers
	fmt.Println("Starting...")
	areatracker.Start(logger, areatracker.Area{Long: 4.3, Lat: 3.6}, *config)
}
