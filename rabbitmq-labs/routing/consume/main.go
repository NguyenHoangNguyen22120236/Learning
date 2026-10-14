
package main

import (
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: consume <queue>")
	}

	queue := os.Args[1]

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

	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		queue, "", false, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening to %s", queue)

	for msg := range msgs {
		log.Printf(
			"Queue=%s | key=%s | body=%s",
			queue, msg.RoutingKey, msg.Body,
		)
		if err := msg.Ack(false); err != nil {
			log.Println("ACK failed:", err)
		}
	}
}
