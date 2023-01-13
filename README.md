# fog-amqp-consumer
[![standard-readme compliant](https://img.shields.io/badge/licence-Apache%202.0-blue)](https://www.apache.org/licenses/LICENSE-2.0) [![standard-readme compliant](https://img.shields.io/static/v1?label=official&message=demo&color=<COLOR>)](https://app.fogcloud.io)

fogcloud amqp consumer

## Installation

```bash
go get github.com/fogcloud-io/fog-amqp-consumer
```

## Quick Start

```golang
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

	err = cli.ConsumeWithHandler(100, key, "fog-test", true, false, false, false, nil, func(b amqp.Delivery) { log.Printf("amqp receive: %s", b.Body) })
	if err != nil {
		log.Print(err)
	}
}

```