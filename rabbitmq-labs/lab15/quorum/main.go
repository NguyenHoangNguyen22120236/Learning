
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
		"lab15.quorum",
		true, false, false, false,
		amqp.Table{
			"x-queue-type":              "quorum",
			"x-delivery-limit":          int32(3),
			"x-dead-letter-exchange":    "lab15.dead.ex",
			"x-dead-letter-routing-key": "dead",
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Quorum queue ready")
}
