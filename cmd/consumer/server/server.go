// Based on [Implementing WebSockets in Golang: Real-Time Communication for Modern Applications | by Puran Adhikari | WiseMonks](https://medium.com/wisemonks/implementing-websockets-in-golang-d3e8e219733b)
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sync"

	consumerapp "github.com/cauan745/trabalho_kafka/internal/app/consumer"
	"github.com/cauan745/trabalho_kafka/internal/kafka/producer"
	"github.com/cauan745/trabalho_kafka/internal/kafka/shared"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var (
	clients   = make(map[*websocket.Conn]bool) // Connected clients
	broadcast = make(chan []byte)              // Broadcast channel
	mutex     = &sync.Mutex{}                  // Protect clients map

)

func wsHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Error upgrading:", err)
		return
	}
	defer conn.Close()

	mutex.Lock()
	clients[conn] = true
	mutex.Unlock()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			mutex.Lock()
			delete(clients, conn)
			mutex.Unlock()
			break
		}

		fmt.Println("Received message", string(message))

		broadcast <- []byte("Oi, bão?")
	}
}

func handleMessages() {
	for {
		// Grab the next message from the broadcast channel
		message := <-broadcast

		// Send the message to all connected clients
		mutex.Lock()
		for client := range clients {
			err := client.WriteMessage(websocket.TextMessage, message)
			if err != nil {
				client.Close()
				delete(clients, client)
			}
		}
		mutex.Unlock()
	}
}

func main() {
	http.HandleFunc("/ws", wsHandler)
	go handleMessages()

	// Kafka
	topic := flag.String("topic", "local_topic", "Kafka Topic Name")
	consumerGroup := flag.String("consumerGroup", "local_cg_server", "Kafka Consumer Group Name")
	host := flag.String("host", "localhost:9092", "Kafka Host Address ex: 'localhost:9092'")

	flag.Parse()

	config := shared.NewKafkaConfig(*topic, *consumerGroup, *host)
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Ride requests producer
	producerCfg := shared.NewKafkaConfig("ride_requests", *consumerGroup, *host)
	rideProducer := producer.NewKafkaProducer("ride_requests", logger, *producerCfg)

	go StartHttpServer(rideProducer)

	// Start consumers
	fmt.Println("Starting...")
	consumerCh := make(chan string)

	err := consumerapp.NewConsumer(consumerCh, logger, *config)
	if err != nil {
		logger.Error(err.Error())
	}

	go func() {
		for msg := range consumerCh {
			broadcast <- []byte(msg)
		}
	}()

	fmt.Println("WebSocket server started on :8000")
	err = http.ListenAndServe(":8000", nil)
	if err != nil {
		fmt.Println("Error starting server:", err)
	}
}
