package main

import (
	"encoding/csv"
	//"encoding/json"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
)

const (
	exchangeName = "exchange"
)

func failOnError(err error, msg string) {
	if err != nil {
		e := fmt.Sprintf("%s: %s", msg, err)
		log.Fatal(e)
		panic(e)
	}
}

type DockDoorEvent struct {
	Topic string
	Timestamp uint
	Message string
}

func main() {
	var amqpUrl, exchangeName, queueName, captureFile string

	flag.StringVar(&amqpUrl, "amqpUrl", "amqp://guest:guest@localhost:5672/", "Url to reach AMQP")
	flag.StringVar(&exchangeName, "exchangeName", "exchange", "Name of exchange")
	flag.StringVar(&queueName, "queueName", "queue", "Name of queue")
	flag.StringVar(&captureFile, "captureFile", "capture.csv", "File to save captured events")
	flag.Parse()

	conn, err := amqp.Dial(amqpUrl)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

//	err = ch.ExchangeDeclare(
//		exchangeName,
//		"topic",
//		true,
//		false,
//		false,
//		false,
//		nil)
//	failOnError(err, "Failed to declare an exchange")

//	q, err := ch.QueueDeclare(
//		queueName,
//		false,
//		false,
//		true,
//		false,
//		nil)
//	failOnError(err, "Failed to declare a queue")

//	err = ch.QueueBind(
//		queueName,
//		"*",
//		exchangeName,
//		false,
//		nil)
//	failOnError(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		queueName,
		"",
		true,
		false,
		false,
		false,
		nil)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		fd, err := os.Create(captureFile)
		failOnError(err, "Failed to open capture file")
		defer fd.Close()

		writer := csv.NewWriter(fd)

		for d := range msgs {
			log.Printf(" [x] %s", d.Body)
			err = writer.Write([]string{
				d.RoutingKey,
				string(d.Body)})
			failOnError(err, "Failed to csv encode event")

			writer.Flush()
		}

//		for d := range msgs {
//			v := DockDoorEvent{
//				Topic: d.RoutingKey,
//				Timestamp: 0,
//				Message: string(d.Body),
//			}
//			b, err := json.Marshal(v)
//			failOnError(err, "Failed to marshal dock door event to json")
//
//			fmt.Fprintln(fd, string(b))
//		}
	}()

	log.Printf(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}
