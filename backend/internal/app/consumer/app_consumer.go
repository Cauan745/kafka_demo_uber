package consumerapp

import (
	"log/slog"

	"github.com/cauan745/trabalho_kafka/internal/kafka/consumer"
	"github.com/cauan745/trabalho_kafka/internal/kafka/shared"
)

func NewConsumer(consumerCh chan<- string, logger *slog.Logger, cfg shared.KafkaConfig) error {
	_, err := consumer.NewKafkaConsumer(consumerCh, logger, cfg)
	if err != nil {
		return err
	}

	return nil
}
