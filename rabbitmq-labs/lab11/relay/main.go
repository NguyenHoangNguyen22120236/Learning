
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	_ "modernc.org/sqlite"
)

func main() {
	db, err := sql.Open("sqlite", "lab11.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var id, eventType, payload string

	err = db.QueryRow(`
		SELECT id, event_type, payload
		FROM outbox_events
		WHERE status = 'PENDING'
		ORDER BY rowid
		LIMIT 1
	`).Scan(&id, &eventType, &payload)

	if err == sql.ErrNoRows {
		log.Println("No pending events")
		return
	}
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Found pending event: %s", id)

	conn, err := amqp.Dial(
		"amqp://guest:guest@localhost:5672/",
	)
	if err != nil {
		log.Fatal("RabbitMQ unavailable: ", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	// Ensure the destination exists for this lab.
	_, err = ch.QueueDeclare(
		"lab11.events",
		true, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	if err := ch.Confirm(false); err != nil {
		log.Fatal(err)
	}

	returns := ch.NotifyReturn(
		make(chan amqp.Return, 1),
	)

	ctx, cancel := context.WithTimeout(
		context.Background(), 10*time.Second,
	)
	defer cancel()

	confirm, err := ch.PublishWithDeferredConfirmWithContext(
		ctx,
		"",
		"lab11.events",
		true, // Mandatory
		false,
		amqp.Publishing{
			MessageId:    id,
			Type:         eventType,
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         []byte(payload),
		},
	)
	if err != nil {
		log.Fatal(err)
	}

	ack, err := confirm.WaitContext(ctx)
	if err != nil {
		log.Fatal("Confirmation unknown: ", err)
	}
	if !ack {
		log.Fatal("RabbitMQ negatively confirmed event")
	}

	select {
	case returned := <-returns:
		log.Fatal(fmt.Errorf(
			"Unroutable message: %s",
			returned.ReplyText,
		))
	default:
	}

	log.Println("RabbitMQ confirmed publication")

	// Deliberately crash AFTER confirm but BEFORE DB update.
	if os.Getenv("CRASH_AFTER_CONFIRM") == "true" {
		log.Println("CRASH before marking PUBLISHED")
		os.Exit(1)
	}

	result, err := db.Exec(`
		UPDATE outbox_events
		SET status = 'PUBLISHED'
		WHERE id = ? AND status = 'PENDING'
	`, id)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		log.Fatal(err)
	}
	if rows != 1 {
		log.Fatal("Outbox state changed unexpectedly")
	}

	log.Printf("Event %s marked PUBLISHED", id)
}
