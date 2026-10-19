
package main

import (
	"database/sql"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	_ "modernc.org/sqlite"
)

func main() {
	db, err := sql.Open("sqlite", "lab8.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Reset the lab data.
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS accounts (
			id TEXT PRIMARY KEY,
			balance INTEGER NOT NULL
		);

		CREATE TABLE IF NOT EXISTS processed_events (
			event_id TEXT PRIMARY KEY
		);

		DELETE FROM processed_events;
		DELETE FROM accounts;

		INSERT INTO accounts (id, balance)
		VALUES ('ACC-001', 1000);
	`)
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

	_, err = ch.QueueDeclare(
		"lab8.payments",
		true, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Clear old messages from previous experiments.
	_, err = ch.QueuePurge("lab8.payments", false)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Lab 8 ready | initial balance=1000")
}
