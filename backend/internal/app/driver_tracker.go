// Package app
package app

import (
	"math/rand"
	"time"
)

type Position struct {
	Latitude  float64 `json:"Latitude"`
	Longitude float64 `json:"Longitude"`
}

func IniciarTracking(ch chan<- Position) {
	for {
		time.Sleep(1 * time.Second)
		ch <- Position{
			Latitude:  rand.Float64() * 90,
			Longitude: rand.Float64() * 180,
		}
	}
}
