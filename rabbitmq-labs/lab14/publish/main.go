
package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	if len(os.Args) != 5 {
		log.Fatal(
			"Usage: publish <queue> <body> <ttl-ms|0> <priority>",
		)
	}

	queue := os.Args[1]
	body := os.Args[2]

	ttl, err := strconv.Atoi(os.Args[3])
	if err != nil || ttl < 0 {
		log.Fatal("Invalid TTL")
	}

	priority, err := strconv.Atoi(os.Args[4])
	if err != nil || priority < 0 || priority > 255 {
		log.Fatal("Invalid priority")
	}

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

	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	msg := amqp.Publishing{
		Body:         []byte(body),
		ContentType:  "text/plain",
		DeliveryMode: amqp.Persistent,
		Priority:     uint8(priority),
	}

	if ttl > 0 {
		msg.Expiration = strconv.Itoa(ttl)
	}

	confirm, err := ch.PublishWithDeferredConfirmWithContext(
		ctx, "", queue, true, false, msg,
	)
	if err != nil {
		log.Fatal(err)
	}

	ok, err := confirm.WaitContext(ctx)
	if err != nil || !ok {
		log.Fatalf("Publish not confirmed: %v", err)
	}

	log.Printf(
		"Confirmed: queue=%s body=%s ttl=%d priority=%d",
		queue, body, ttl, priority,
	)
}
