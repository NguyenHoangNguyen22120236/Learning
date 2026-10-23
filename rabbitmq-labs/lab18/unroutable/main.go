
package main

import (
	"context"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
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
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	confirm, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		"",                       // Default exchange
		"lab18.queue.not.exists", // No matching queue
		true,                    // Deliberately wrong
		false,
		amqp.Publishing{
			Body:         []byte("order.created"),
			DeliveryMode: amqp.Persistent,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	ok, err := confirm.WaitContext(ctx)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Publisher confirmation ACK=%v", ok)

	select {
	case ret := <-returns:
		log.Printf("Returned: %s", ret.ReplyText)
	default:
		log.Println("No basic.return received")
	}
}
