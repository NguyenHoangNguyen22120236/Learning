
package main

import (
	"context"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: publish <exchange> <routing-key>")
	}

	exchange := os.Args[1]
	key := os.Args[2]

	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	err = ch.PublishWithContext(
		ctx,
		exchange,
		key,
		false, // mandatory (test later)
		false, // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:         []byte("Event: " + key),
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Publish attempted: %s -> %s", exchange, key)
}
