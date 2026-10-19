
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
		true, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	closed := conn.NotifyClose(
		make(chan *amqp.Error, 1),
	)

	msgs, err := ch.Consume(
		"lab9.jobs", "",
		false, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Connected. Waiting for messages...")

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				log.Println("Delivery channel closed")
				return
			}

			log.Printf("Received: %s", msg.Body)
			if err := msg.Ack(false); err != nil {
				log.Printf("ACK error: %v", err)
				return
			}

		case err, ok := <-closed:
			if ok {
				log.Printf("Connection lost: %v", err)
			} else {
				log.Println("Connection closed")
			}
			return
		}
	}
}
