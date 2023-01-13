package main

import (
	"log"

	consumer "github.com/fogcloud-io/fog-amqp-consumer"
	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	AMQPHost     = "localhost"
	AMQPPort     = "5672"
	AMQPTLS      = false
	accessKey    = "xgHc40bf04fb020c"
	accessSecret = "c3bad348bb34390558f7f1aacce17877"
	clientID     = "fog-consumer"
)

func main() {
	cli, err := consumer.NewFogConsumerClient(AMQPHost, AMQPPort, accessKey, accessSecret, clientID, AMQPTLS)
	if err != nil {
		log.Fatal(err)
	}

	err = cli.ConsumeWithHandler(100, accessKey, "fog-test", false, false, false, false, nil, func(b amqp.Delivery) { log.Printf("amqp receive: %s", b.Body); b.Ack(true) })
	if err != nil {
		log.Print(err)
	}
}
