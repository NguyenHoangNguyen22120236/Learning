
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
		"events.direct",
		"direct",
		true,  // durable
		false, // auto-delete
		false, // internal
		false, // no-wait
		nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	bindings := []struct {
		queue string
		key   string
	}{
		{"orders.queue", "order.created"},
		{"payments.queue", "payment.completed"},
	}

	for _, b := range bindings {
		_, err := ch.QueueDeclare(
			b.queue, true, false, false, false, nil,
		)
		if err != nil {
			log.Fatal(err)
		}

		err = ch.QueueBind(
			b.queue,
			b.key,
			"events.direct",
			false,
			nil,
		)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Bound %s to %s", b.queue, b.key)
	}
}
