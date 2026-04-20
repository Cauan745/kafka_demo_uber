// Package app
package app

import (
	crypto "crypto/rand"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"
)

type Driver struct {
	DriverId float64  `json:"driverId"`
	HexColor string   `json:"hexColor"`
	Position Position `json:"position"`
}

type Position struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

func IniciarTracking(ch chan<- Driver, passengerPosition Position, color string) {
	driverId := rand.Float64() * 1000000

	pos := Position{
		Latitude:  GenerateRandomLatitude(),
		Longitude: GenerateRandomLongitude(),
	}

	js, err := json.Marshal(passengerPosition)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println(string(js))

	for {
		time.Sleep(1 * time.Second)

		pos.approximateTo(passengerPosition)

		ch <- Driver{
			DriverId: driverId,
			HexColor: color,
			Position: pos,
		}
	}
}

// Increments or decrecements latitude or longitude approximating to destination based on a car at 60 mph at the equator
func (p *Position) approximateTo(destination Position) {
	latSpeed := 0.00024  // 60 mph. * 60 turns into hour speed
	longSpeed := 0.00024 // 60 mph at equator. * 60 ...

	if p.Latitude == destination.Latitude && p.Longitude == destination.Longitude {
		os.Exit(1)
	}

	if math.Abs(p.Latitude-destination.Latitude) < latSpeed {
		p.Latitude = destination.Latitude
	} else if p.Latitude-destination.Latitude > 0 {
		p.Latitude -= latSpeed
	} else if p.Latitude-destination.Latitude < 0 {
		p.Latitude += latSpeed
	}

	if math.Abs(p.Longitude-destination.Longitude) < longSpeed {
		p.Longitude = destination.Longitude
	} else if p.Longitude-destination.Longitude > 0 {
		p.Longitude -= longSpeed
	} else if p.Longitude-destination.Longitude < 0 {
		p.Longitude += longSpeed
	}
}

// GenerateHexColor generates a random 6-character hex color string.
func GenerateHexColor() (string, error) {
	bytes := make([]byte, 3)
	if _, err := crypto.Read(bytes); err != nil {
		return "", err
	}
	// %02x formats numbers to base-16 with at least 2 digits
	return fmt.Sprintf("#%02x%02x%02x", bytes[0], bytes[1], bytes[2]), nil
}

// Generated Within Porto Velho
func GenerateRandomLatitude() float64 {
	lat := -8.7636

	if math.Round(rand.Float64()) == 1 {
		lat -= rand.Float64() * 0.05
	} else {
		lat += rand.Float64() * 0.05
	}

	return lat
}

// Generated Within Porto Velho
func GenerateRandomLongitude() float64 {
	long := -63.8972

	if math.Round(rand.Float64()) == 1 {
		long += rand.Float64() * 0.1
	} else {
		long -= rand.Float64() * 0.05
	}

	return long
}
