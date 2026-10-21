
package main

import (
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

	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		"lab15.quorum", "",
		false, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Quorum poison-message test started")

	for msg := range msgs {
		log.Printf(
			"Received=%s | redelivered=%v",
			msg.Body, msg.Redelivered,
		)
		log.Printf(
			"x-delivery-count=%v | x-acquired-count=%v",
			msg.Headers["x-delivery-count"],
			msg.Headers["x-acquired-count"],
		)

		time.Sleep(time.Second)

		log.Println("REJECT -> REQUEUE")
		if err := msg.Reject(true); err != nil {
			log.Fatal(err)
		}
	}
}
