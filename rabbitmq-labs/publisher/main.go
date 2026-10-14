
package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
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
		"hello_queue",
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Fatal(err)
	}

	r := gin.Default()

	r.POST("/publish", func(c *gin.Context) {
		var body struct {
			Message string `json:"message" binding:"required"`
		}

		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		ctx, cancel := context.WithTimeout(
			c.Request.Context(), 5*time.Second,
		)
		defer cancel()

		err := ch.PublishWithContext(
			ctx,
			"",            // default exchange
			"hello_queue", // routing key
			false,         // mandatory
			false,         // immediate (deprecated)
			amqp.Publishing{
				ContentType:  "text/plain",
				DeliveryMode: amqp.Persistent,
				Body:         []byte(body.Message),
			},
		)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable,
				gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusAccepted, gin.H{
			"status": "publish attempted",
		})
	})

	log.Fatal(r.Run(":8080"))
}
