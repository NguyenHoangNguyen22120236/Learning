
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type PaymentEvent struct {
	EventID   string `json:"eventId"`
	AccountID string `json:"accountId"`
	Amount    int64  `json:"amount"`
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Usage: publish <eventId> <amount>")
	}

	amount, err := strconv.ParseInt(os.Args[2], 10, 64)
	if err != nil || amount <= 0 {
		log.Fatal("Amount must be a positive integer")
	}

	event := PaymentEvent{
		EventID:   os.Args[1],
		AccountID: "ACC-001",
		Amount:    amount,
	}

	body, err := json.Marshal(event)
	if err != nil {
		log.Fatal(err)
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

	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	confirm, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		"",
		"lab8.payments",
		true,
		false,
		amqp.Publishing{
			MessageId:    event.EventID,
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	confirmed, err := confirm.WaitContext(ctx)
	if err != nil || !confirmed {
		log.Fatalf("Publish not confirmed: %v", err)
	}

	log.Printf("Published: %+v", event)
}
