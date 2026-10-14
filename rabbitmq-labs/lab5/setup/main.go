
package main

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
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

	queues := []struct {
		name    string
		durable bool
	}{
		{"lab5.temporary", false},
		{"lab5.durable", true},
	}

	for _, q := range queues {
		_, err := ch.QueueDeclare(
			q.name, q.durable,
			false, false, false, nil,
		)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf(
			"Declared %s | durable=%v",
			q.name, q.durable,
		)
	}
}
