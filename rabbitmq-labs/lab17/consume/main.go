
package main

import (
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func argInt(index int) int {
	if len(os.Args) <= index {
		log.Fatal("Missing argument")
	}

	n, err := strconv.Atoi(os.Args[index])
	if err != nil || n < 1 {
		log.Fatal("Argument must be positive")
	}
	return n
}

func main() {
	if len(os.Args) != 5 {
		log.Fatal(
			"Usage: consume <count> <workers> <prefetch> <delay-ms>",
		)
	}

	count := argInt(1)
	workers := argInt(2)
	prefetch := argInt(3)
	delayMS := argInt(4)

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
		"lab17.benchmark",
		"",
		false, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	jobs := make(chan amqp.Delivery, workers)
	var wg sync.WaitGroup
	var completed atomic.Int64

	start := time.Now()

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(workerID int) {
			defer wg.Done()

			for msg := range jobs {
				time.Sleep(
					time.Duration(delayMS) * time.Millisecond,
				)

				// ACK only this delivery.
				if err := msg.Ack(false); err != nil {
					log.Printf(
						"Worker %d ACK error: %v",
						workerID, err,
					)
					continue
				}

				completed.Add(1)
			}
		}(i + 1)
	}

	// This lab assumes exactly count queued messages,
	// one consumer process, and no competing consumers.
	for i := 0; i < count; i++ {
		msg, ok := <-msgs
		if !ok {
			log.Fatal("Delivery channel closed")
		}

		jobs <- msg
	}

	close(jobs)
	wg.Wait()

	elapsed := time.Since(start)

	log.Printf(
		"Workers=%d Prefetch=%d Completed=%d Time=%v Throughput=%.2f msg/s",
		workers,
		prefetch,
		completed.Load(),
		elapsed,
		float64(completed.Load())/elapsed.Seconds(),
	)
}
