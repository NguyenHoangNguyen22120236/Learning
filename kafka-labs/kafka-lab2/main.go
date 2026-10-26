
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/twmb/franz-go/pkg/kgo"
)

func main() {
    client, err := kgo.NewClient(
        kgo.SeedBrokers("localhost:9092"),
        kgo.DefaultProduceTopic("lab2.payments"),
        kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    events := []struct {
        Key   string
        Value string
    }{
		{"account-104", "Deposit 500"},
		{"account-104", "Withdraw 200"},
    }

    for _, event := range events {
        ctx, cancel := context.WithTimeout(
            context.Background(),
            10*time.Second,
        )

        results := client.ProduceSync(
            ctx,
            &kgo.Record{
                Key:   []byte(event.Key),
                Value: []byte(event.Value),
            },
        )
        cancel()

        if err := results.FirstErr(); err != nil {
            log.Fatal("produce failed: ", err)
        }

        record := results[0].Record

        fmt.Printf(
            "key=%s partition=%d offset=%d value=%s\n",
            string(record.Key),
            record.Partition,
            record.Offset,
            string(record.Value),
        )
    }
}
