package main

import (
    "context"
    "errors"
    "fmt"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"
)

func main() {
    mux := http.NewServeMux()

    mux.HandleFunc("/", func(
        w http.ResponseWriter,
        r *http.Request,
    ) {
        fmt.Println("Request started")

        time.Sleep(5 * time.Second)

        fmt.Fprintln(w, "Completed")
        fmt.Println("Request finished")
    })

    server := &http.Server{
        Addr:    ":8080",
        Handler: mux,
    }

    serveDone := make(chan struct{})

    go func() {
        defer close(serveDone)

        err := server.ListenAndServe()

        if err != nil &&
            !errors.Is(err, http.ErrServerClosed) {
            fmt.Println("Server error:", err)
        }

        fmt.Println("End Go Func")
    }()

    signalCtx, stop := signal.NotifyContext(
        context.Background(),
        os.Interrupt,
        syscall.SIGTERM,
    )
    defer stop()

    <-signalCtx.Done()
    stop()

    fmt.Println("Shutdown requested")

    shutdownCtx, cancel := context.WithTimeout(
        context.Background(),
        10*time.Second,
    )
    defer cancel()

    if err := server.Shutdown(shutdownCtx); err != nil {
        fmt.Println("Shutdown error:", err)
    }

    <-serveDone
    fmt.Println("Process exiting")
}

