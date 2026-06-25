// Package producerapp
package producerapp

import (
	"encoding/json"
	"math/rand"

	"github.com/cauan745/trabalho_kafka/internal/app"
	"github.com/cauan745/trabalho_kafka/internal/kafka/producer"
)

// driverQuantity is the amount of driver_tracker goroutine to generate
func Start(producer *producer.KafkaProducer, driverQuantity int, consumerCh chan string) {
	producerCh := make(chan app.Driver)

	endCh := make(chan bool)

	for range driverQuantity {
		generateTracker(producerCh, producer, endCh)
	}

	go producerLoop(producerCh, producer)

	if consumerCh != nil {
		go func() {
			for msg := range consumerCh {
				type RideRequest struct {
					PassengerId float64 `json:"passengerId"`
					Latitude    float64 `json:"latitude"`
					Longitude   float64 `json:"longitude"`
				}
				var req RideRequest
				err := json.Unmarshal([]byte(msg), &req)
				if err != nil {
					continue
				}

				passengerPos := app.Position{Latitude: req.Latitude, Longitude: req.Longitude}
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

				pas := Passenger{Id: req.PassengerId, Latitude: passengerPos.Latitude, Longitude: passengerPos.Longitude, Color: color}
				js, err := json.Marshal(pas)
				if err == nil {
					producer.Produce(string(js))
				}
			}
		}()
	}

	for end := range endCh {
		if !end {
			continue
		}

		driverQuantity--

		if driverQuantity == 0 {
			if consumerCh == nil {
				return
			}
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

	passengerPos := app.Position{
		Latitude:  app.GenerateRandomLatitude(),
		Longitude: app.GenerateRandomLongitude(),
	}

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
