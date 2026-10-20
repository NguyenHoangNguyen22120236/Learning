
package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial(
		"amqp://guest:guest@localhost:5673/",
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

	q, err := ch.QueueDeclare(
		"lab12.quorum",
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false,
		amqp.Table{
			"x-queue-type":                 "quorum",
			"x-quorum-initial-group-size": int32(3),
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Created quorum queue: %s", q.Name)
}
