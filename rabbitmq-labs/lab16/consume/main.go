
package main

import (
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	mode := os.Getenv("MODE")
	if mode == "" {
		mode = "normal"
	}

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

	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		"lab16.jobs",
		"",
		false, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Consumer started | mode=%s", mode)

	for msg := range msgs {
		log.Printf(
			"RECEIVED %s | redelivered=%v",
			msg.Body, msg.Redelivered,
		)

		switch mode {
		case "stuck":
			log.Println("Simulating stuck processing")
			// Hold the delivery without an ACK.
			select {}

		case "slow":
			time.Sleep(5 * time.Second)

		default:
			time.Sleep(100 * time.Millisecond)
		}

		log.Printf("FINISHED %s", msg.Body)

		if err := msg.Ack(false); err != nil {
			log.Fatal(err)
		}
	}
}
