// Package producerapp
package producerapp

import (
	"encoding/json"
	"fmt"

	"github.com/cauan745/trabalho_kafka/internal/app"
	"github.com/cauan745/trabalho_kafka/internal/kafka/producer"
)

func Start(producer *producer.KafkaProducer) {
	// Producer code
	producerCh := make(chan app.Position)
	go app.IniciarTracking(producerCh)

	for pos := range producerCh {
		// fmt.Println("Latitude:", pos.Latitude, "Longitude:", pos.Longitude)
		//lat := fmt.Sprint("Latitude:", strconv.FormatFloat(pos.Latitude, 'f', -1, 64))
		//long := fmt.Sprint("Longitude:", strconv.FormatFloat(pos.Longitude, 'f', -1, 64))
		//

		j, err := json.Marshal(pos)
		if err != nil {
			fmt.Println("Erro ao converter Position para json")
			continue
		}
		producer.Produce(string(j))
	}
}
