
package main

import (
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	port := "5673"
	if len(os.Args) > 1 {
		port = os.Args[1]
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

	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		"lab12.quorum",
		"",
		false, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Waiting for quorum queue messages")

	for msg := range msgs {
		log.Printf(
			"Received: %s | redelivered=%v",
			msg.Body, msg.Redelivered,
		)

		if err := msg.Ack(false); err != nil {
			log.Fatal(err)
		}
	}
}
