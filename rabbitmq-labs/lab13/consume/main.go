
package main

import (
	"log"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal(
			"Usage: consume <queue> <delay-ms> [prefetch]",
		)
	}

	queue := os.Args[1]

	delayMS, err := strconv.Atoi(os.Args[2])
	if err != nil || delayMS < 0 {
		log.Fatal("invalid delay")
	}

	prefetch := 1
	if len(os.Args) >= 4 {
		prefetch, err = strconv.Atoi(os.Args[3])
		if err != nil || prefetch < 1 {
			log.Fatal("invalid prefetch")
		}
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

	if err := ch.Qos(prefetch, 0, false); err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		queue, "",
		false, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf(
		"Consumer started | delay=%dms | prefetch=%d",
		delayMS, prefetch,
	)

	for msg := range msgs {
		log.Printf("START %s", msg.Body)

		time.Sleep(
			time.Duration(delayMS) * time.Millisecond,
		)

		log.Printf("FINISH %s", msg.Body)

		if err := msg.Ack(false); err != nil {
			log.Fatal(err)
		}
	}
}
