package consumer

import (
	"context"
	"log"
	"testing"
)

var (
	host   = "localhost"
	port   = "5672"
	key    = "xgHc40bf04fb020c"
	secret = "c3bad348bb34390558f7f1aacce17877"
)

func TestConsumerWithHandler(t *testing.T) {
	cli := InitAMQPConsumer(context.Background(), host, port, key, secret)
	cli.ConsumeWithHandler(context.Background(), func(b []byte) { log.Printf("amqp receive: %s", b) })
}
