
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

	err = ch.ExchangeDeclare(
		"events.fanout", "fanout",
		true, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	for _, queue := range []string{
		"notification.queue",
		"analytics.queue",
	} {
		_, err = ch.QueueDeclare(
			queue, true, false, false, false, nil,
		)
		if err != nil {
			log.Fatal(err)
		}

		err = ch.QueueBind(
			queue, "", "events.fanout", false, nil,
		)
		if err != nil {
			log.Fatal(err)
		}

		log.Println("Bound:", queue)
	}
}
