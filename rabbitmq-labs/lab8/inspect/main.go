
package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "modernc.org/sqlite"
)

func main() {
	db, err := sql.Open("sqlite", "lab8.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var balance int64
	err = db.QueryRow(
		"SELECT balance FROM accounts WHERE id = ?",
		"ACC-001",
	).Scan(&balance)
	if err != nil {
		log.Fatal(err)
	}

	var count int
	err = db.QueryRow(
		"SELECT COUNT(*) FROM processed_events",
	).Scan(&count)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Balance: %d\n", balance)
	fmt.Printf("Processed event IDs: %d\n", count)
}
