// Package producerapp
package producerapp

import (
	"encoding/json"
	"fmt"
	"math/rand"

	"github.com/cauan745/trabalho_kafka/internal/app"
	"github.com/cauan745/trabalho_kafka/internal/kafka/producer"
)

func Start(producer *producer.KafkaProducer) {
	// Producer code
	producerCh := make(chan app.Driver)

	passengerPos := app.Position{app.GenerateRandomLatitude(), app.GenerateRandomLongitude()}

	color, err := app.GenerateHexColor()
	if err != nil {
		color = "#FFFFFF"
	}

	go app.IniciarTracking(producerCh, passengerPos, color)

	type Passenger struct {
		Id        float64 `json:"passengerId"`
		Latitude  float64 `json:"latitude"`
		Longitude float64 `json:"longitude"`
		Color     string  `json:"hexColor"`
	}

	pas := Passenger{Id: rand.Float64() * 1000000, Latitude: passengerPos.Latitude, Longitude: passengerPos.Longitude, Color: color}

	js, err := json.Marshal(pas)
	if err != nil {
		panic(err.Error())
	}

	producer.Produce(string(js))

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

		fmt.Println(string(j))
		fmt.Println(string(js))
	}
}
