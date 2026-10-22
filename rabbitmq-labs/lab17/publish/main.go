
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const queue = "lab17.benchmark"

func main() {
	if len(os.Args) != 4 {
		log.Fatal(
			"Usage: publish <sequential|batch> <count> <batch-size>",
		)
	}

	mode := os.Args[1]

	if mode != "sequential" && mode != "batch" {
		log.Fatal("Mode must be sequential or batch")
	}

	count, err := strconv.Atoi(os.Args[2])
	if err != nil || count < 1 {
		log.Fatal("Invalid message count")
	}

	batchSize, err := strconv.Atoi(os.Args[3])
	if err != nil || batchSize < 1 {
		log.Fatal("Invalid batch size")
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

	if err := ch.Confirm(false); err != nil {
		log.Fatal(err)
	}

	// This benchmark uses a predeclared destination
	// and one publisher on one AMQP channel.
	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Minute,
	)
	defer cancel()

	confirms := make([]*amqp.DeferredConfirmation, 0, batchSize)

	waitBatch := func() error {
		for _, confirm := range confirms {
			ok, err := confirm.WaitContext(ctx)
			if err != nil {
				return fmt.Errorf("confirm wait: %w", err)
			}
			if !ok {
				return fmt.Errorf(
					"broker NACKed publication %d",
					confirm.DeliveryTag,
				)
			}
		}

		confirms = confirms[:0]
		return nil
	}

	start := time.Now()

	for i := 1; i <= count; i++ {
		body := []byte(fmt.Sprintf("Job-%08d", i))

		confirm, err := ch.PublishWithDeferredConfirmWithContext(
			ctx,
			"",
			queue,
			false, // Destination exists in this controlled test
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				MessageId:    fmt.Sprintf("lab17-%d", i),
				Body:         body,
			},
		)
		if err != nil {
			log.Fatal(err)
		}

		confirms = append(confirms, confirm)

		if mode == "sequential" || len(confirms) == batchSize {
			if err := waitBatch(); err != nil {
				log.Fatal(err)
			}
		}
	}

	// Confirm the final partial batch.
	if len(confirms) > 0 {
		if err := waitBatch(); err != nil {
			log.Fatal(err)
		}
	}

	elapsed := time.Since(start)

	log.Printf(
		"Mode=%s Count=%d Batch=%d Time=%s Throughput=%.2f msg/s",
		mode,
		count,
		batchSize,
		elapsed,
		float64(count)/elapsed.Seconds(),
	)
}
