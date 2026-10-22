
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
		"lab17.benchmark",
		true, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	_, err = ch.QueuePurge("lab17.benchmark", false)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Benchmark queue is ready")
}
