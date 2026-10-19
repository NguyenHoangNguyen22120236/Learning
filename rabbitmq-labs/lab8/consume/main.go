
package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	_ "modernc.org/sqlite"
)

type PaymentEvent struct {
	EventID   string `json:"eventId"`
	AccountID string `json:"accountId"`
	Amount    int64  `json:"amount"`
}

func process(db *sql.DB, event PaymentEvent) (bool, error) {
	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	if os.Getenv("NO_IDEMPOTENCY") != "true" {
		result, err := tx.Exec(`
			INSERT OR IGNORE INTO processed_events(event_id)
			VALUES (?)
		`, event.EventID)
		if err != nil {
			return false, err
		}

		rows, err := result.RowsAffected()
		if err != nil {
			return false, err
		}
		if rows == 0 {
			return false, nil
		}
	}

	// Business operation in the SAME transaction.
	result, err := tx.Exec(`
		UPDATE accounts
		SET balance = balance + ?
		WHERE id = ?
	`, event.Amount, event.AccountID)
	if err != nil {
		return false, err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if rows != 1 {
		return false, errors.New("account not found")
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}

	return true, nil
}

func main() {
	name := os.Getenv("WORKER_NAME")
	if name == "" {
		name = "worker"
	}

	db, err := sql.Open(
		"sqlite",
		"file:lab8.db?_pragma=busy_timeout(10000)",
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.SetMaxOpenConns(1)

	conn, err := amqp.DialConfig(
		"amqp://guest:guest@localhost:5672/",
		amqp.Config{
			Heartbeat: 10 * time.Minute,
		},
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

	if err := ch.Qos(1, 0, false); err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(
		"lab8.payments",
		"",
		false, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Started consumer: %s", name)

	for msg := range msgs {
		var event PaymentEvent

		if err := json.Unmarshal(msg.Body, &event); err != nil {
			log.Printf("Invalid JSON: %v", err)
			if err := msg.Reject(false); err != nil {
				log.Fatal(err)
			}
			continue
		}

		if event.EventID == "" ||
			event.AccountID == "" ||
			event.Amount <= 0 {
			log.Println("Invalid payment event")
			if err := msg.Reject(false); err != nil {
				log.Fatal(err)
			}
			continue
		}

		// Allows us to demonstrate competing consumers.
		if os.Getenv("SLOW") == "true" {
			time.Sleep(2 * time.Second)
		}

		applied, err := process(db, event)
		if err != nil {
			log.Printf("DB error: %v", err)

			// Keep the delivery unacked.
			// Closing the connection will requeue it.
			return
		}

		if applied {
			log.Printf("[%s] APPLIED %s",
				name, event.EventID)
		} else {
			log.Printf("[%s] DUPLICATE %s",
				name, event.EventID)
		}

		// Crash AFTER transaction commit, BEFORE ACK.
		if os.Getenv("CRASH_AFTER_COMMIT") == "true" {
			log.Println("Simulated crash before ACK")
			os.Exit(1)
		}

		if err := msg.Ack(false); err != nil {
			log.Fatal(err)
		}
	}
}
