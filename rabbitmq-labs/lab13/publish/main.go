
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

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: publish <queue> <count> [delay-ms]")
	}

	queue := os.Args[1]

	count, err := strconv.Atoi(os.Args[2])
	if err != nil || count <= 0 {
		log.Fatal("count must be positive")
	}

	delayMS := 0
	if len(os.Args) >= 4 {
		delayMS, err = strconv.Atoi(os.Args[3])
		if err != nil || delayMS < 0 {
			log.Fatal("invalid delay")
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

	if err := ch.Confirm(false); err != nil {
		log.Fatal(err)
	}

	returns := ch.NotifyReturn(
		make(chan amqp.Return, 1),
	)

	start := time.Now()
	acked, nacked := 0, 0

	for i := 1; i <= count; i++ {
		ctx, cancel := context.WithTimeout(
			context.Background(), 10*time.Second,
		)

		body := fmt.Sprintf("Job-%03d", i)

		confirm, err := ch.PublishWithDeferredConfirmWithContext(
			ctx,
			"",
			queue,
			true, false,
			amqp.Publishing{
				MessageId:    body,
				DeliveryMode: amqp.Persistent,
				Body:         []byte(body),
			},
		)
		if err != nil {
			cancel()
			log.Fatal("Publish failed: ", err)
		}

		ok, err := confirm.WaitContext(ctx)
		cancel()

		if err != nil {
			log.Fatal("Confirmation outcome unknown: ", err)
		}

		if ok {
			acked++
		} else {
			nacked++
		}

		select {
		case ret := <-returns:
			log.Fatalf("Unroutable: %s", ret.ReplyText)
		default:
		}

		log.Printf(
			"%s | confirmed=%t",
			body, ok,
		)

		if delayMS > 0 {
			time.Sleep(
				time.Duration(delayMS) * time.Millisecond,
			)
		}
	}

	elapsed := time.Since(start)

	log.Printf(
		"DONE | ACK=%d NACK=%d elapsed=%v",
		acked, nacked, elapsed,
	)
}
