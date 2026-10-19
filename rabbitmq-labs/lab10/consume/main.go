
package main

import (
	"log"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

import "sync"

func main() {
	var wg sync.WaitGroup

	name := os.Getenv("WORKER_NAME")
	if name == "" {
		name = "worker"
	}

	queue := os.Getenv("QUEUE")
	if queue == "" {
		queue = "lab10.orders"
	}

	prefetch := 1
	if os.Getenv("PREFETCH") == "10" {
		prefetch = 10
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

	if err := ch.Qos(prefetch, 0, false); err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		queue, name,
		false, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("%s started | queue=%s", name, queue)
	
	for msg := range msgs {
		number, err := strconv.Atoi(string(msg.Body))
		if err != nil {
			log.Fatal(err)
		}

		wg.Add(1)

		go func(msg amqp.Delivery, number int) {
			defer wg.Done()

			log.Printf("START Job-%d", number)

			// Deterministic processing-time variation.
			delay := time.Duration(11-number) * 300 * time.Millisecond
			time.Sleep(delay)

			log.Printf("FINISH Job-%d", number)

			if err := msg.Ack(false); err != nil {
				log.Printf("ACK error: %v", err)
			}
		}(msg, number)
	}

	wg.Wait()

}
