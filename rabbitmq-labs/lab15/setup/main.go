
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

	// Main exchange receives retry messages
	// when their TTL expires.
	if err := ch.ExchangeDeclare(
		"lab15.main.ex", "direct",
		true, false, false, false, nil,
	); err != nil {
		log.Fatal(err)
	}

	// Final dead-letter exchange.
	if err := ch.ExchangeDeclare(
		"lab15.dead.ex", "direct",
		true, false, false, false, nil,
	); err != nil {
		log.Fatal(err)
	}

	type QueueConfig struct {
		name string
		args amqp.Table
	}

	queues := []QueueConfig{
		{
			name: "lab15.main",
			args: amqp.Table{
				"x-dead-letter-exchange":    "lab15.dead.ex",
				"x-dead-letter-routing-key": "dead",
			},
		},
		{
			name: "lab15.retry.5s",
			args: amqp.Table{
				"x-message-ttl":             int32(5000),
				"x-dead-letter-exchange":    "lab15.main.ex",
				"x-dead-letter-routing-key": "main",
			},
		},
		{
			name: "lab15.retry.10s",
			args: amqp.Table{
				"x-message-ttl":             int32(10000),
				"x-dead-letter-exchange":    "lab15.main.ex",
				"x-dead-letter-routing-key": "main",
			},
		},
		{
			name: "lab15.retry.20s",
			args: amqp.Table{
				"x-message-ttl":             int32(20000),
				"x-dead-letter-exchange":    "lab15.main.ex",
				"x-dead-letter-routing-key": "main",
			},
		},
		{name: "lab15.dead"},
	}

	for _, q := range queues {
		_, err := ch.QueueDeclare(
			q.name, true,
			false, false, false, q.args,
		)
		if err != nil {
			log.Fatal(err)
		}

		log.Println("Declared:", q.name)
	}

	if err := ch.QueueBind(
		"lab15.main", "main",
		"lab15.main.ex", false, nil,
	); err != nil {
		log.Fatal(err)
	}

	if err := ch.QueueBind(
		"lab15.dead", "dead",
		"lab15.dead.ex", false, nil,
	); err != nil {
		log.Fatal(err)
	}

	log.Println("Lab 15 topology ready")
}
