package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
)

const (
	ordersTopic = "oranges"
)

type Orange struct {
	Size int `json:"size"`
}

type OrangeCounter struct {
	Small  int
	Medium int
	Large  int
}

func (oc *OrangeCounter) Add(size int) {
	if size < 10 {
		oc.Small++
	} else if size < 20 {
		oc.Medium++
	} else {
		oc.Large++
	}
}

func (oc *OrangeCounter) String() string {
	return fmt.Sprintf("Oranges: small=%d, medium=%d, large=%d", oc.Small, oc.Medium, oc.Large)
}

func main() {
	ctx := context.Background()

	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   ordersTopic,
		GroupID: "orange-sorter",
	})

	oc := &OrangeCounter{}

	go func() {
		for {
			message, err := kafkaReader.ReadMessage(ctx)
			if err != nil {
				log.Fatal().Err(err).Msg("Failed to read message")
			}

			var orange Orange
			if err := json.Unmarshal(message.Value, &orange); err != nil {
				log.Warn().Err(err).Msg("Failed to decode message")
				continue
			}

			oc.Add(orange.Size)
		}
	}()

	for {
		time.Sleep(10 * time.Second)
		log.Info().Msg(oc.String())
	}
}
