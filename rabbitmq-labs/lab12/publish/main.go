
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	port := "5673"
	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	count := 5
	if len(os.Args) > 2 {
		n, err := strconv.Atoi(os.Args[2])
		if err != nil || n < 1 {
			log.Fatal("Invalid count")
		}
		count = n
	}

	url := fmt.Sprintf(
		"amqp://guest:guest@localhost:%s/", port,
	)

	conn, err := amqp.Dial(url)
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

	for i := 1; i <= count; i++ {
		ctx, cancel := context.WithTimeout(
			context.Background(), 10*time.Second,
		)

		body := fmt.Sprintf(
			"Job-%d-%d", time.Now().UnixNano(), i,
		)

		confirm, err := ch.PublishWithDeferredConfirmWithContext(
			ctx,
			"",
			"lab12.quorum",
			true,
			false,
			amqp.Publishing{
				MessageId:    body,
				DeliveryMode: amqp.Persistent,
				Body:         []byte(body),
			},
		)
		if err != nil {
			cancel()
			log.Fatal(err)
		}

		ok, err := confirm.WaitContext(ctx)
		cancel()
		if err != nil || !ok {
			log.Fatalf("Publish not confirmed: %v", err)
		}

		select {
		case ret := <-returns:
			log.Fatalf("Unroutable: %s", ret.ReplyText)
		default:
		}

		log.Printf("CONFIRMED %s", body)
	}
}
