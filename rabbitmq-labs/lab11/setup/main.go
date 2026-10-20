
package main

import (
	"database/sql"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	_ "modernc.org/sqlite"
)

func main() {
	db, err := sql.Open("sqlite", "lab11.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS orders (
			id TEXT PRIMARY KEY,
			status TEXT NOT NULL
		);

		CREATE TABLE IF NOT EXISTS outbox_events (
			id TEXT PRIMARY KEY,
			event_type TEXT NOT NULL,
			payload TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'PENDING'
		);
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
		"lab11.events",
		true, false, false, false, nil,
	)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Lab 11 setup complete")
}
