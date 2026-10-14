
package main

import (
	"context"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Usage: publish <main|retry> <message>")
	}

	destination := os.Args[1]
	body := os.Args[2]

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

	if err := ch.Confirm(false); err != nil {
		log.Fatal(err)
	}

	// Use the default exchange to address a queue directly.
	var queue string
	switch destination {
	case "main":
		queue = "lab6.main"
	case "retry":
		queue = "lab6.retry"
	default:
		log.Fatal("Expected main or retry")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	confirm, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		"",
		queue,
		false, false,
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(body),
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	ack, err := confirm.WaitContext(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if !ack {
		log.Fatal("Publisher NACK received")
	}

	log.Printf("Published to %s: %s", queue, body)
}
