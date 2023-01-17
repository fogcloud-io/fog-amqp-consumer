package main

import (
	"log"

	consumer "github.com/fogcloud-io/fog-amqp-consumer"
	"go.uber.org/zap"
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
	l, _ := zap.NewDevelopment()
	cli, err := consumer.NewFogConsumerClient(AMQPHost, AMQPPort, accessKey, accessSecret, clientID, AMQPTLS, consumer.WithClientOptionsLogger(l.Sugar()))
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		err = cli.ConsumeWithHandler(100, accessKey, "fog-test", func(b consumer.Delivery) { log.Printf("amqp receive: %s", b.Body); b.Ack(true) })
		if err != nil {
			log.Print(err)
		}
	}()

	ch := make(chan struct{})
	<-ch

}
