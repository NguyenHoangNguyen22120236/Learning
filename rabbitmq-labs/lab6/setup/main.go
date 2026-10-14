
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

	// 1. Declare exchanges.
	exchanges := []string{
		"lab6.main.ex",
		"lab6.dead.ex",
	}

	for _, name := range exchanges {
		err := ch.ExchangeDeclare(
			name, "direct",
			true, false, false, false, nil,
		)
		if err != nil {
			log.Fatal(err)
		}
	}

	// 2. Main queue: rejected messages go to DLQ.
	_, err = ch.QueueDeclare(
		"lab6.main",
		true, false, false, false,
		amqp.Table{
			"x-dead-letter-exchange":    "lab6.dead.ex",
			"x-dead-letter-routing-key": "dead",
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	// 3. Retry queue: messages expire after 5 seconds
	// and return to the main queue.
	_, err = ch.QueueDeclare(
		"lab6.retry",
		true, false, false, false,
		amqp.Table{
			"x-message-ttl":             int32(5000),
			"x-dead-letter-exchange":    "lab6.main.ex",
			"x-dead-letter-routing-key": "main",
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	// 4. Dead-letter queue.
	_, err = ch.QueueDeclare(
		"lab6.dead",
		true, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	// 5. Exchange bindings.
	bindings := []struct {
		queue    string
		key      string
		exchange string
	}{
		{"lab6.main", "main", "lab6.main.ex"},
		{"lab6.dead", "dead", "lab6.dead.ex"},
	}

	for _, b := range bindings {
		err := ch.QueueBind(
			b.queue, b.key, b.exchange, false, nil,
		)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Println("Lab 6 topology ready")
}
