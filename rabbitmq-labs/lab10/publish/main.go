
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	queue := "lab10.orders"
	if len(os.Args) > 1 {
		queue = os.Args[1]
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

	for i := 1; i <= 10; i++ {
		ctx, cancel := context.WithTimeout(
			context.Background(), 10*time.Second,
		)

		confirm, err := ch.PublishWithDeferredConfirmWithContext(
			ctx,
			"",
			queue,
			true, false,
			amqp.Publishing{
				MessageId:    fmt.Sprintf("job-%02d", i),
				DeliveryMode: amqp.Persistent,
				Body:         []byte(fmt.Sprintf("%d", i)),
			},
		)
		if err != nil {
			cancel()
			log.Fatal(err)
		}

		ok, err := confirm.WaitContext(ctx)
		cancel()
		if err != nil || !ok {
			log.Fatalf("Publish confirmation failed: %v", err)
		}

		log.Printf("Published Job-%d", i)
	}
}
