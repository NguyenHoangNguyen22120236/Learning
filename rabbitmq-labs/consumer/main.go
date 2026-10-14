
package main

import (
	"log"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value < 0 {
		return fallback
	}
	return value
}

func main() {
	name := os.Getenv("WORKER_NAME")
	if name == "" {
		name = "worker"
	}

	processTime := envInt("PROCESS_TIME", 1)
	prefetch := envInt("PREFETCH", 1)

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

	_, err = ch.QueueDeclare(
		"hello_queue",
		true, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	err = ch.Qos(prefetch, 0, false)
	if err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		"hello_queue",
		name,
		false, // manual ACK
		false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf(
		"Started: %s | delay=%ds | prefetch=%d",
		name, processTime, prefetch,
	)

	for msg := range msgs {
		log.Printf("[%s] START %s",
			name, string(msg.Body))

		time.Sleep(time.Duration(processTime) * time.Second)

		log.Printf("[%s] END %s",
			name, string(msg.Body))

		if err := msg.Ack(false); err != nil {
			log.Printf("[%s] ACK failed: %v", name, err)
		}
	}
}
