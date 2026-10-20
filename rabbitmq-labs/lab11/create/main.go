
package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"

	_ "modernc.org/sqlite"
)

type OrderCreated struct {
	EventID string `json:"eventId"`
	OrderID string `json:"orderId"`
	Status  string `json:"status"`
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Usage: create <order-id>")
	}

	orderID := os.Args[1]
	eventID := "evt-" + orderID

	db, err := sql.Open("sqlite", "lab11.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	_, err = tx.Exec(`
		INSERT INTO orders(id, status)
		VALUES (?, 'CREATED')
	`, orderID)
	if err != nil {
		log.Fatal(err)
	}

	event := OrderCreated{
		EventID: eventID,
		OrderID: orderID,
		Status:  "CREATED",
	}

	payload, err := json.Marshal(event)
	if err != nil {
		log.Fatal(err)
	}

	_, err = tx.Exec(`
		INSERT INTO outbox_events(
			id, event_type, payload, status
		) VALUES (?, 'order.created', ?, 'PENDING')
	`, eventID, string(payload))
	if err != nil {
		log.Fatal(err)
	}

	// Simulate a failure before commit.
	if os.Getenv("ROLLBACK") == "true" {
		log.Println("Simulated failure -> ROLLBACK")
		return
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	log.Printf("Committed order=%s and event=%s",
		orderID, eventID)
}
