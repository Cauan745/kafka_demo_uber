// Package driverlogger
package driverlogger

import (
	"fmt"
	"log/slog"

	consumerapp "github.com/cauan745/trabalho_kafka/internal/app/consumer"
	"github.com/cauan745/trabalho_kafka/internal/kafka/shared"
)

func Start(logger *slog.Logger, cfg shared.KafkaConfig) {
	consumerCh := make(chan string)
	err := consumerapp.NewConsumer(consumerCh, logger, cfg)
	if err != nil {
		logger.Error(err.Error())
	}

	func() {
		for msg := range consumerCh {
			fmt.Println(msg)
		}
	}()
}
