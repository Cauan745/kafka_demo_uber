// Package areatracker
package areatracker

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"

	"github.com/cauan745/trabalho_kafka/internal/app"
	consumerapp "github.com/cauan745/trabalho_kafka/internal/app/consumer"
	"github.com/cauan745/trabalho_kafka/internal/kafka/shared"
)

type Area struct {
	Long float64
	Lat  float64
}

func Start(logger *slog.Logger, area Area, cfg shared.KafkaConfig) {
	consumerCh := make(chan string)

	err := consumerapp.NewConsumer(consumerCh, logger, cfg)
	if err != nil {
		logger.Error(err.Error())
	}

	func() {
		for msg := range consumerCh {

			pos := app.Position{}

			err := json.Unmarshal([]byte(msg), &pos)
			if err != nil {
				continue
			}

			closeness := math.Abs(pos.Latitude-area.Lat) + math.Abs(pos.Longitude-area.Long)

			if closeness <= 50 {
				fmt.Println("Driver is close to area!")
			}

		}
	}()
}
