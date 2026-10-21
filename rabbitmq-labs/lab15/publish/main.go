
package main

import (
	"context"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: publish <message>")
	}

	body := os.Args[1]

	conn, err := amqp.Dial(
		"amqp://guest:guest@localhost:5672/",
	)
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
		"",
		"lab15.main",
		true, false,
		amqp.Publishing{
			MessageId:    body,
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			Headers: amqp.Table{
				"attempt": int32(1),
			},
			Body: []byte(body),
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	ok, err := confirm.WaitContext(ctx)
	if err != nil || !ok {
		log.Fatalf("Publish not confirmed: %v", err)
	}

	select {
	case returned := <-returns:
		log.Fatalf("Unroutable: %s", returned.ReplyText)
	default:
		log.Printf("CONFIRMED %s", body)
	}
}
