// Package producerapp
package producerapp

import (
	"encoding/json"
	"math/rand"
	"os"

	"github.com/cauan745/trabalho_kafka/internal/app"
	"github.com/cauan745/trabalho_kafka/internal/kafka/producer"
)

// driverQuantity is the amount of driver_tracker goroutine to generate
func Start(producer *producer.KafkaProducer, driverQuantity int) {
	producerCh := make(chan app.Driver)

	endCh := make(chan bool)

	for range driverQuantity {
		generateTracker(producerCh, producer, endCh)
	}

	go producerLoop(producerCh, producer)

	for end := range endCh {
		if end {
			driverQuantity--
		}

		if driverQuantity == 0 {
			os.Exit(1)
		}
	}
}

func producerLoop(producerCh chan app.Driver, producer *producer.KafkaProducer) {
	for pos := range producerCh {
		// fmt.Println("Latitude:", pos.Latitude, "Longitude:", pos.Longitude)
		//lat := fmt.Sprint("Latitude:", strconv.FormatFloat(pos.Latitude, 'f', -1, 64))
		//long := fmt.Sprint("Longitude:", strconv.FormatFloat(pos.Longitude, 'f', -1, 64))
		//

		j, err := json.Marshal(pos)
		if err != nil {
			// fmt.Println("Erro ao converter Position para json")
			continue
		}
		producer.Produce(string(j))

		// fmt.Println(string(j))
	}
}

func generateTracker(producerCh chan app.Driver, producer *producer.KafkaProducer, endCh chan bool) {
	// Producer code

	passengerPos := app.Position{app.GenerateRandomLatitude(), app.GenerateRandomLongitude()}

	color, err := app.GenerateHexColor()
	if err != nil {
		color = "#FFFFFF"
	}

	go app.IniciarTracking(producerCh, passengerPos, color, endCh)

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
}
