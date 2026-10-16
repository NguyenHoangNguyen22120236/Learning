
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const maxAttempts = 3

var (
	errTemporary = errors.New("temporary failure")
	errPermanent = errors.New("permanent failure")
)

func process(body string, attempt int) error {
	switch {
	case strings.HasPrefix(body, "temporary"):
		// Succeeds from the second attempt onwards.
		if attempt == 1 {
			return errTemporary
		}
		return nil

	case strings.HasPrefix(body, "permanent"):
		return errPermanent

	case strings.HasPrefix(body, "poison"):
		return errTemporary

	default:
		return nil
	}
}

func retry(
	ch *amqp.Channel,
	returns <-chan amqp.Return,
	msg amqp.Delivery,
	attempt int,
) error {
	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	confirm, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		"",             // Default exchange
		"lab6.retry",   // Retry queue
		true,           // Mandatory
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: amqp.Persistent,
			MessageId:    msg.MessageId,
			Body:         msg.Body,
			Headers: amqp.Table{
				"attempt": int32(attempt + 1),
			},
		},
	)
	if err != nil {
		return err
	}

	ack, err := confirm.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !ack {
		return errors.New("retry publication NACKed")
	}

	// One outstanding publication at a time.
	// RabbitMQ sends basic.return before its confirm.
	select {
	case returned := <-returns:
		return errors.New(
			"unroutable retry: " + returned.ReplyText,
		)
	default:
		return nil
	}
}

func main() {
	conn, err := amqp.Dial(
		"amqp://guest:guest@localhost:5672/?heartbeat=300",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Separate channels for consuming and publishing.
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
		"lab6.main",
		"",
		false, // Manual ACK
		false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Lab 7 retry worker started")

	for msg := range msgs {
		attempt := 1

		if value, ok := msg.Headers["attempt"]; ok {
			if n, ok := value.(int32); ok {
				attempt = int(n)
			}
		}

		body := string(msg.Body)

		log.Printf(
			"Processing %s | attempt=%d",
			body, attempt,
		)

		err := process(body, attempt)

		if err == nil {
			log.Println("SUCCESS -> ACK")
			if err := msg.Ack(false); err != nil {
				log.Fatal(err)
			}
			continue
		}

		// Permanent failures or exhausted retries.
		if errors.Is(err, errPermanent) ||
			attempt >= maxAttempts {
			log.Println("FINAL FAILURE -> DLQ")

			if err := msg.Nack(false, false); err != nil {
				log.Fatal(err)
			}
			continue
		}

		log.Println("TEMPORARY FAILURE -> RETRY")

		// Publish first, ACK original afterwards.
		if err := retry(
			publisherCh, returns, msg, attempt,
		); err != nil {
			log.Printf(
				"Retry publishing failed or uncertain: %v",
				err,
			)

			// Stop the worker without ACK.
			// Connection closure requeues the original.
			return
		}

		// Simulate a crash AFTER confirmed retry publish,
		// but BEFORE acknowledging the original.
		if os.Getenv("CRASH_AFTER_CONFIRM") == "true" {
			log.Println("Simulated crash before original ACK")
			os.Exit(1)
		}

		if err := msg.Ack(false); err != nil {
			log.Fatal(err)
		}

		log.Println("Retry scheduled -> original ACKed")
	}
}
