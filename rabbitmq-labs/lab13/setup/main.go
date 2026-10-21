
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

	queues := []struct {
		name string
		args amqp.Table
	}{
		{
			name: "lab13.unlimited",
			args: nil,
		},
		{
			name: "lab13.drop",
			args: amqp.Table{
				"x-max-length": int32(5),
				"x-overflow":   "drop-head",
			},
		},
		{
			name: "lab13.reject",
			args: amqp.Table{
				"x-max-length": int32(5),
				"x-overflow":   "reject-publish",
			},
		},
	}

	for _, q := range queues {
		_, err := ch.QueueDeclare(
			q.name,
			true, false, false, false,
			q.args,
		)
		if err != nil {
			log.Fatal(err)
		}

		_, err = ch.QueuePurge(q.name, false)
		if err != nil {
			log.Fatal(err)
		}

		log.Println("Ready:", q.name)
	}
}
