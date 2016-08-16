package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		e := fmt.Sprintf("%s: %s", msg, err)
		log.Fatal(e)
		panic(e)
	}
}

func main() {
	var freq, cycle time.Duration
	var amqpUrl, exchangeName, replayFile string

	flag.DurationVar(&freq, "freq", 100*time.Millisecond, "Event emission frequency (must be acceptable by time.ParseDuration)")
	flag.DurationVar(&cycle, "cycle", 5*time.Second, "Sleep time between event script cycles (must be acceptable by time.ParseDuration")
	flag.StringVar(&amqpUrl, "amqpUrl", "amqp://guest:guest@localhost:5672/", "Url for amqp (amqp://guest:guest@localhost:5672/")
	flag.StringVar(&exchangeName, "exchangeName", "exchange", "Name of exchange to use for dock door events")
	flag.StringVar(&replayFile, "replayFile", "replay.csv", "CSV file containing replay data")
	flag.Parse()

	conn, err := amqp.Dial(amqpUrl)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		exchangeName,
		"topic",
		true,
		false,
		false,
		false,
		nil)
	failOnError(err, "Failed to declare an exchange")

	fd, err := os.Open(replayFile)
	failOnError(err, fmt.Sprintf("Failed to open %s", replayFile))

	reader := csv.NewReader(fd)
	records, err := reader.ReadAll()
	failOnError(err, "Failed reading file")

	for {
		for _, rec := range records {
			if len(rec) != 2 {
				log.Fatalf("Record is not correctly formatted: %v\n", rec)
			}

			err = ch.Publish(
				exchangeName,
				rec[0],
				false,
				false,
				amqp.Publishing{
					ContentType: "application/json",
					Body: []byte(rec[1]),
				})
			failOnError(err, "Failed to publish")

			time.Sleep(freq)
		}

		time.Sleep(cycle)
	}

	return
}
