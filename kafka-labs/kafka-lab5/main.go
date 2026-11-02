
package main

import (
    "context"
    "flag"
    "fmt"
    "log"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "github.com/twmb/franz-go/pkg/kgo"
)

func main() {
    mode := flag.String("mode", "async", "sync or async")
    count := flag.Int("count", 1000, "number of records")
    linger := flag.Duration("linger", 10*time.Millisecond,
        "producer linger")
    codec := flag.String("compression", "none",
        "none or snappy")
    flag.Parse()

    var compression kgo.CompressionCodec
    switch *codec {
    case "none":
        compression = kgo.NoCompression()
    case "snappy":
        compression = kgo.SnappyCompression()
    default:
        log.Fatal("unsupported compression")
    }

    client, err := kgo.NewClient(
        kgo.SeedBrokers("localhost:9092"),
        kgo.DefaultProduceTopic("lab5.producer"),
        kgo.ProducerLinger(*linger),
        kgo.ProducerBatchCompression(compression),
        kgo.ProducerBatchMaxBytes(256*1024),
        kgo.MaxBufferedRecords(500),
        kgo.MaxBufferedBytes(4*1024*1024),
        kgo.RecordDeliveryTimeout(30*time.Second),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    if *mode != "sync" && *mode != "async" {
        log.Fatal("mode must be sync or async")
    }

    var succeeded atomic.Int64
    var failed atomic.Int64

    payload := []byte(strings.Repeat(
        `{"type":"PaymentCreated","merchant":"store-101"},`,
        20,
    ))

    started := time.Now()

    var wg sync.WaitGroup

    for i := 0; i < *count; i++ {
        record := &kgo.Record{
            Key: []byte(fmt.Sprintf("payment-%06d", i)),
            Value: payload,
        }

        if *mode == "sync" {
            results := client.ProduceSync(
                context.Background(),
                record,
            )

            if err := results.FirstErr(); err != nil {
                failed.Add(1)
                log.Printf("produce failed: %v", err)
            } else {
                succeeded.Add(1)
            }
            continue
        }

        wg.Add(1)

        client.Produce(
            context.Background(),
            record,
            func(r *kgo.Record, err error) {
                defer wg.Done()

                if err != nil {
                    failed.Add(1)
                    log.Printf("produce failed: %v", err)
                    return
                }

                succeeded.Add(1)
            },
        )
    }

    // Ensure all asynchronous production callbacks finish.
    wg.Wait()

    elapsed := time.Since(started)

    fmt.Printf("\nMode: %s\n", *mode)
    fmt.Printf("Compression: %s\n", *codec)
    fmt.Printf("Linger: %s\n", *linger)
    fmt.Printf("Success: %d\n", succeeded.Load())
    fmt.Printf("Failed: %d\n", failed.Load())
    fmt.Printf("Elapsed: %s\n", elapsed)
    fmt.Printf("Records/sec: %.2f\n",
        float64(*count)/elapsed.Seconds())
}
