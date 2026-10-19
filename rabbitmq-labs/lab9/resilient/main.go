
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	amqpURL = "amqp://guest:guest@localhost:5672/"
	queue   = "lab9.jobs"
)

func main() {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	log.Println("Resilient consumer starting")

	for ctx.Err() == nil {
		err := consumeSession(ctx)
		if ctx.Err() != nil {
			break
		}

		log.Printf("Session ended: %v", err)
		log.Println("Reconnecting in 2 seconds...")

		select {
		case <-ctx.Done():
		case <-time.After(2 * time.Second):
		}
	}

	log.Println("Consumer shutdown complete")
}

func consumeSession(ctx context.Context) error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	_, err = ch.QueueDeclare(
		queue, true, false, false, false, nil,
	)
	if err != nil {
		return err
	}

	if err := ch.Qos(1, 0, false); err != nil {
		return err
	}

	const consumerTag = "lab9-worker"

	msgs, err := ch.Consume(
		queue, consumerTag,
		false, false, false, false, nil,
	)
	if err != nil {
		return err
	}

	connClosed := conn.NotifyClose(
		make(chan *amqp.Error, 1),
	)
	chClosed := ch.NotifyClose(
		make(chan *amqp.Error, 1),
	)

	log.Println("CONNECTED -> consuming messages")

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutdown requested")

			// Stop new deliveries from RabbitMQ.
			if err := ch.Cancel(consumerTag, false); err != nil {
				log.Printf("Cancel error: %v", err)
			}
			return nil

		case err, ok := <-connClosed:
			if !ok {
				return fmt.Errorf("connection closed")
			}
			return fmt.Errorf("connection lost: %v", err)

		case err, ok := <-chClosed:
			if !ok {
				return fmt.Errorf("channel closed")
			}
			return fmt.Errorf("channel lost: %v", err)

		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("deliveries closed")
			}

			// If shutdown arrived during selection,
			// do not begin another business operation.
			if ctx.Err() != nil {
				_ = msg.Nack(false, true)
				return nil
			}

			log.Printf(
				"START %s | redelivered=%v",
				string(msg.Body),
				msg.Redelivered,
			)

			// Simulate a slow business operation.
			time.Sleep(8 * time.Second)

			log.Printf("FINISH %s", string(msg.Body))

			if err := msg.Ack(false); err != nil {
				return fmt.Errorf("ack: %w", err)
			}

			log.Println("ACK sent")
		}
	}
}
