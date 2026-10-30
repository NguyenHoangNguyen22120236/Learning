
package main

import (
    "context"
    "crypto/rand"
    "fmt"
    "log"
    "time"

    "github.com/twmb/franz-go/pkg/kgo"
)

func main() {
    client, err := kgo.NewClient(
        kgo.SeedBrokers("localhost:9092"),
        kgo.DefaultProduceTopic("lab4.storage"),

        // Keep this experiment uncompressed.
        kgo.ProducerBatchCompression(kgo.NoCompression()),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    const (
        totalRecords = 3000
        payloadSize  = 2048
    )

    // Random payloads avoid extreme compression and
    // make this a more useful disk-size experiment.
    payload := make([]byte, payloadSize)
    if _, err := rand.Read(payload); err != nil {
        log.Fatal(err)
    }

    start := time.Now()

    for i := 0; i < totalRecords; i++ {
        record := &kgo.Record{
            Key:   []byte(fmt.Sprintf("event-%d", i)),
            Value: append([]byte(nil), payload...),
        }

        ctx, cancel := context.WithTimeout(
            context.Background(),
            10*time.Second,
        )

        results := client.ProduceSync(ctx, record)
        cancel()

        if err := results.FirstErr(); err != nil {
            log.Fatalf("produce %d failed: %v", i, err)
        }

        if i%500 == 0 {
            r := results[0].Record

            fmt.Printf(
                "i=%d partition=%d offset=%d\n",
                i, r.Partition, r.Offset,
            )
        }
    }

    elapsed := time.Since(start)

    fmt.Printf(
        "Produced %d records in %v\n",
        totalRecords,
        elapsed,
    )
}
