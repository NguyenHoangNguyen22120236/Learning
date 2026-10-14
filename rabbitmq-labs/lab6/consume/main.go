
package main

import (
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	mode := os.Getenv("MODE")
	if mode == "" {
		mode = "success"
	}

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

	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		"lab6.main",
		"",
		false, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Consumer started | mode=%s", mode)

	for msg := range msgs {
		log.Printf("Received: %s", string(msg.Body))
		log.Printf("Redelivered: %v", msg.Redelivered)
		log.Printf("Headers: %+v", msg.Headers)

		switch mode {
		case "fail":
			log.Println("Processing failed -> NACK(false, false)")
			err = msg.Nack(false, false)

		case "inspect":
			log.Println("Inspecting message -> ACK")
			err = msg.Ack(false)

		default:
			log.Println("Processing successful -> ACK")
			err = msg.Ack(false)
		}

		if err != nil {
			log.Fatal(err)
		}
	}
}
