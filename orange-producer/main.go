package main

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

const (
	ordersTopic = "oranges"
	numOranges  = 100
)

type Orange struct {
	Size int `json:"size"`
}

func main() {
	ctx := context.Background()
	conn, err := kafka.DialLeader(ctx, "tcp", "localhost:9092", ordersTopic, 0)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to dial kafka")
	}
	defer conn.Close()

	for i := 0; i < numOranges; i++ {
		size := rand.Intn(30) + 1
		orange := Orange{Size: size}
		message, err := json.Marshal(orange)
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to marshal orange")
		}
		_, err = conn.WriteMessages(kafka.Message{Value: message})
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to write kafka messages")
		}
		time.Sleep(100 * time.Millisecond)
	}
}
