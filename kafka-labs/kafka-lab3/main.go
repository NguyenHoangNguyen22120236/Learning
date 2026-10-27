
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/twmb/franz-go/pkg/kgo"
)

func processOrder(record *kgo.Record) error {
    fmt.Printf(
        "PROCESS topic=%s partition=%d offset=%d value=%s\n",
        record.Topic,
        record.Partition,
        record.Offset,
        string(record.Value),
    )

    // Simulate a database operation.
    time.Sleep(500 * time.Millisecond)

    return nil
}

func main() {
    client, err := kgo.NewClient(
        kgo.SeedBrokers("localhost:9092"),
        kgo.ConsumerGroup("lab3-crash-worker"),
        kgo.ConsumeTopics("lab3.offsets"),
        kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
        kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
        kgo.DisableAutoCommit(),

        // Prevent partition revocation while processing
        // a fetched batch.
        kgo.BlockRebalanceOnPoll(),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()

    for {
        fetches := client.PollRecords(ctx, 1)

        if fetches.IsClientClosed() {
            return
        }

        if errs := fetches.Errors(); len(errs) > 0 {
            for _, fetchErr := range errs {
                log.Printf("fetch error: %v", fetchErr.Err)
            }
            client.AllowRebalance()
            continue
        }

        records := fetches.Records()

        if len(records) == 0 {
            client.AllowRebalance()
            continue
        }

        record := records[0]

        if err := processOrder(record); err != nil {
            client.AllowRebalance()
            log.Fatalf("processing failed: %v", err)
        }

        // Simulate abrupt process termination.
        // The business operation completed, but no offset was committed.
        panic("SIMULATED CRASH BEFORE COMMIT")

        commitCtx, cancel := context.WithTimeout(
            context.Background(),
            10*time.Second,
        )

        err := client.CommitRecords(commitCtx, record)
        cancel()

        if err != nil {
            client.AllowRebalance()
            log.Fatalf("commit failed: %v", err)
        }

        fmt.Printf(
            "COMMITTED next_offset=%d\n",
            record.Offset+1,
        )

        client.AllowRebalance()
    }
}
