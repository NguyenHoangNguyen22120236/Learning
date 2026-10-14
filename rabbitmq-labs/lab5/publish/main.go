
package main

import (
	"context"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	if len(os.Args) != 4 {
		log.Fatal(
			"Usage: publish <queue> <persistent|transient> <message>",
		)
	}

	queue := os.Args[1]
	mode := os.Args[2]
	body := os.Args[3]

	deliveryMode := amqp.Transient
	switch mode {
	case "persistent":
		deliveryMode = amqp.Persistent
	case "transient":
		// Use default transient mode.
	default:
		log.Fatal("Mode must be persistent or transient")
	}

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

	returns := ch.NotifyReturn(
		make(chan amqp.Return, 1),
	)

	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	confirm, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		"", queue,
		true, false,
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: deliveryMode,
			Body:         []byte(body),
		},
	)
	
	if err != nil {
		log.Fatal(err)
	}

	ok, err := confirm.WaitContext(ctx)
	if err != nil {
		log.Fatal("Publication outcome unknown: ", err)
	}
	if !ok {
		log.Fatal("Broker negatively confirmed publication")
	}

	// For this single-message experiment, RabbitMQ sends
	// basic.return before the publisher confirm when unroutable.
	select {
	case returned := <-returns:
		log.Printf(
			"UNROUTABLE: code=%d reason=%s key=%s",
			returned.ReplyCode,
			returned.ReplyText,
			returned.RoutingKey,
		)
	default:
		log.Println("Confirmed and not returned")
	}
}
