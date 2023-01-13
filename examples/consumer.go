package main

import (
	"log"
	"time"

	consumer "github.com/fogcloud-io/fog-amqp-consumer"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	host     = "localhost"
	port     = "5672"
	key      = "xgHc40bf04fb020c"
	secret   = "c3bad348bb34390558f7f1aacce17877"
	clientID = "fog-consumer"
)

func main() {
	cli, err := consumer.NewFogConsumerClient(host, port, key, secret, clientID, false)
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		time.Sleep(time.Second * 3)
		cli.Close()
	}()

	err = cli.ConsumeWithHandler(100, key, "fog-test", true, false, false, false, nil, func(b amqp.Delivery) { log.Printf("amqp receive: %s", b.Body) })
	if err != nil {
		log.Print(err)
	}
}
