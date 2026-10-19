
package main

import (
	"log"

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

	_, err = ch.QueueDeclare(
		"lab9.jobs",
		false, // Deliberately incompatible
		false, false, false, nil,
	)

	log.Printf("QueueDeclare error: %v", err)
	log.Printf("Channel closed: %v", ch.IsClosed())
	log.Printf("Connection closed: %v", conn.IsClosed())
}
