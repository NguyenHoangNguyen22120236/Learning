
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
		name    string
		durable bool
		args    amqp.Table
	}{
		{
			name:    "lab14.ttl",
			durable: true,
			args: amqp.Table{
				"x-message-ttl": int32(5000),
			},
		},
		{
			name:    "lab14.message",
			durable: true,
			args:    nil,
		},
		{
			name:    "lab14.expires",
			durable: false,
			args: amqp.Table{
				"x-expires": int32(10000),
			},
		},
		{
			name:    "lab14.priority",
			durable: true,
			args: amqp.Table{
				"x-max-priority": int32(3),
			},
		},
	}

	for _, q := range queues {
		_, err := ch.QueueDeclare(
			q.name,
			q.durable,
			false, false, false,
			q.args,
		)
		if err != nil {
			log.Fatal(err)
		}

		if q.name != "lab14.expires" {
			_, err = ch.QueuePurge(q.name, false)
			if err != nil {
				log.Fatal(err)
			}
		}

		log.Println("Created:", q.name)
	}
}
