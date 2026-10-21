
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const maxAttempts = 4

var (
	errTemporary = errors.New("temporary failure")
	errPermanent = errors.New("permanent failure")
)

func process(body string, attempt int) error {
	switch {
	case strings.HasPrefix(body, "temporary-"):
		if attempt < 3 {
			return errTemporary
		}
		return nil

	case strings.HasPrefix(body, "poison-"):
		return errTemporary

	case strings.HasPrefix(body, "permanent-"):
		return errPermanent

	default:
		return nil
	}
}

func retryQueue(attempt int) string {
	switch attempt {
	case 1:
		return "lab15.retry.5s"
	case 2:
		return "lab15.retry.10s"
	case 3:
		return "lab15.retry.20s"
	default:
		return ""
	}
}

func publishRetry(
	ch *amqp.Channel,
	returns <-chan amqp.Return,
	msg amqp.Delivery,
	attempt int,
) error {
	queue := retryQueue(attempt)
	if queue == "" {
		return errors.New("no retry queue available")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	confirm, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		"",
		queue,
		true,
		false,
		amqp.Publishing{
			MessageId:    msg.MessageId,
			ContentType:  msg.ContentType,
			DeliveryMode: amqp.Persistent,
			Body:         msg.Body,
			Headers: amqp.Table{
				"attempt": int32(attempt + 1),
			},
		},
	)
	if err != nil {
		return err
	}

	ok, err := confirm.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("confirm unknown: %w", err)
	}
	if !ok {
		return errors.New("broker NACKed retry")
	}

	// This sequential lab has one outstanding publish.
	select {
	case ret := <-returns:
		return fmt.Errorf("unroutable retry: %s", ret.ReplyText)
	default:
		return nil
	}
}

func main() {
	conn, err := amqp.Dial(
		"amqp://guest:guest@localhost:5672/",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	consumerCh, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer consumerCh.Close()

	publisherCh, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer publisherCh.Close()

	if err := publisherCh.Confirm(false); err != nil {
		log.Fatal(err)
	}

	returns := publisherCh.NotifyReturn(
		make(chan amqp.Return, 1),
	)

	if err := consumerCh.Qos(1, 0, false); err != nil {
		log.Fatal(err)
	}

	msgs, err := consumerCh.Consume(
		"lab15.main", "",
		false, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Lab 15 consumer started")

	for msg := range msgs {
		attempt := 1

		if n, ok := msg.Headers["attempt"].(int32); ok {
			attempt = int(n)
		}

		body := string(msg.Body)

		log.Printf(
			"PROCESS %s | attempt=%d | redelivered=%v",
			body, attempt, msg.Redelivered,
		)

		processErr := process(body, attempt)

		if processErr == nil {
			log.Println("SUCCESS -> ACK")
			if err := msg.Ack(false); err != nil {
				log.Fatal(err)
			}
			continue
		}

		if errors.Is(processErr, errPermanent) ||
			attempt >= maxAttempts {
			log.Println("FINAL FAILURE -> DLQ")

			if err := msg.Nack(false, false); err != nil {
				log.Fatal(err)
			}
			continue
		}

		queue := retryQueue(attempt)
		log.Printf("RETRY SCHEDULED -> %s", queue)

		// Publish retry first, confirm it,
		// then acknowledge the original.
		err = publishRetry(
			publisherCh, returns, msg, attempt,
		)
		if err != nil {
			log.Printf("RETRY PUBLISH FAILED: %v", err)
			// Original remains unacknowledged.
			// Closing connection requeues it.
			return
		}

		if os.Getenv("CRASH_AFTER_CONFIRM") == "true" {
			log.Println("SIMULATED CRASH BEFORE ORIGINAL ACK")
			os.Exit(1)
		}

		if err := msg.Ack(false); err != nil {
			log.Fatal(err)
		}

		log.Println("RETRY PUBLISHED -> ORIGINAL ACKED")
	}
}
