package consumer

import (
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
)

type Logger interface {
	Debug(args ...interface{})
	Debugf(template string, args ...interface{})
	Info(args ...interface{})
	Infof(template string, args ...interface{})
	Warn(args ...interface{})
	Warnf(template string, args ...interface{})
	Fatal(args ...interface{})
	Fatalf(template string, args ...interface{})
}

var (
	ErrAmqpShutdown           = errors.New("amqp shutdown")
	ErrAmqpChannelInitTimeout = errors.New("amqp channel init timeout")
	ErrAmqpConnTimeout        = errors.New("amqp connect timeout")
	ErrAmqpConnNil            = errors.New("amqp conn nil")
	ErrAmqpReconn             = errors.New("amqp reconnecting")
)

type (
	ClientOption   func(*RabbitmqClient)
	ConsumerOption func(*consumerParams)
	reconnOption   func(*RabbitmqClient) error

	AMQPMsgHandler = func(amqp.Delivery)
	Delivery       = amqp.Delivery
	Table          = amqp.Table

	consumerParams struct {
		consumerTag string
		autoAck     bool
		exclusive   bool
		noWait      bool
		args        Table
	}
)

func WithClientOptionsLogger(logger Logger) ClientOption {
	return func(rc *RabbitmqClient) {
		if logger != nil {
			rc.logger = logger
		}
	}
}

func WithClientOptionsConnTimeout(t time.Duration) ClientOption {
	return func(rc *RabbitmqClient) {
		rc.connTimeout = t
	}
}

func WithClientOptionsChInitTimeout(t time.Duration) ClientOption {
	return func(rc *RabbitmqClient) {
		rc.initMQChTimeout = t
	}
}

/*
When autoAck (also known as noAck) is true, the server will acknowledge
deliveries to this consumer prior to writing the delivery to the network.  When
autoAck is true, the consumer should not call Delivery.Ack. Automatically
acknowledging deliveries means that some deliveries may get lost if the
consumer is unable to process them after the server delivers them.
See http://www.rabbitmq.com/confirms.html for more details.
*/
func WithConsumerOptionsAutoAck(autoAck bool) ConsumerOption {
	return func(co *consumerParams) {
		co.autoAck = autoAck
	}
}

/*
When exclusive is true, the server will ensure that this is the sole consumer
from this queue. When exclusive is false, the server will fairly distribute
deliveries across multiple consumers.
*/
func WithConsumerOptionsExclusive(exclusive bool) ConsumerOption {
	return func(co *consumerParams) {
		co.exclusive = exclusive
	}
}

/*
When noWait is true, do not wait for the server to confirm the request and
immediately begin deliveries.  If it is not possible to consume, a channel
exception will be raised and the channel will be closed.
*/
func WithConsumerOptionsNoWait(noWait bool) ConsumerOption {
	return func(co *consumerParams) {
		co.noWait = noWait
	}
}

func WithConsumerOptionsConsumerTag(consumerTag string) ConsumerOption {
	return func(co *consumerParams) {
		co.consumerTag = consumerTag
	}
}

func WithConsumerOptionsArgs(args Table) ConsumerOption {
	return func(co *consumerParams) {
		co.args = args
	}
}

func withReconnOptionsMQInit(client *RabbitmqClient) error {
	if client == nil {
		return nil
	}
	ch, err := client.handleMQChInit()
	if err != nil {
		return err
	}
	client.logger.Info("RabbitmqClient init channel successfully")
	client.chgDefaultCh(ch)
	return nil
}

func parseAMQPURL(host, port, key, secret, clientID string, tls bool) string {
	u, p := getAMQPAccess(key, secret, clientID)
	protocol := "amqp"
	if tls {
		protocol = "amqps"
	}
	return fmt.Sprintf("%s://%s:%s@%s:%s", protocol, u, p, host, port)
}

func getAMQPAccess(key, secret, clientID string) (username, password string) {
	timestamp := strconv.Itoa(int(time.Now().Unix()))
	sign, _ := authAMQPSign(clientID, key, timestamp, secret, "hmacsha1")
	username = fmt.Sprintf("%s&%s&%s", clientID, key, timestamp)
	password = sign
	return
}

func authAMQPSign(clientId, accessKey, timestamp, accessSecret, signMethod string) (string, error) {
	src := ""
	src = fmt.Sprintf("clientId%saccessKey%s", clientId, accessKey)
	if timestamp != "" {
		src = src + "timestamp" + timestamp
	}

	var h hash.Hash
	switch signMethod {
	case "hmacsha1":
		h = hmac.New(sha1.New, []byte(accessSecret))
	case "hmacmd5":
		h = hmac.New(md5.New, []byte(accessSecret))
	default:
		return "", errors.New("no access")
	}

	_, err := h.Write([]byte(src))
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func NewFogConsumerClient(host, port, accessKey, accessSecret, clientID string, tls bool, opts ...ClientOption) (*RabbitmqClient, error) {
	uri := parseAMQPURL(host, port, accessKey, accessSecret, clientID, tls)
	return NewRabbitmqCli(uri, opts...)
}

func NewRabbitmqCli(endpoint string, opts ...ClientOption) (*RabbitmqClient, error) {
	l, _ := zap.NewProduction()
	cli := &RabbitmqClient{
		url:             endpoint,
		done:            make(chan bool, 1),
		reconnDone:      make(chan struct{}, 1),
		connTimeout:     time.Second * 32,
		initMQChTimeout: time.Second * 32,
		logger:          l.Sugar(),
	}

	for _, opt := range opts {
		opt(cli)
	}

	err := cli.handleReConnSync(withReconnOptionsMQInit)
	if err != nil {
		return nil, err
	}
	go cli.pingpong()
	return cli, nil
}

type RabbitmqClient struct {
	url        string
	conn       *amqp.Connection
	mqCh       *amqp.Channel
	logger     Logger
	reConnFlag uint32

	connTimeout     time.Duration
	initMQChTimeout time.Duration

	mu sync.Mutex

	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	notifyConfirm   chan amqp.Confirmation

	done       chan bool
	reconnDone chan struct{}
}

func (rc *RabbitmqClient) Close() error {
	close(rc.done)
	if rc.mqCh != nil {
		rc.mqCh.Close()
	}
	if rc.conn != nil {
		rc.conn.Close()
	}
	return nil
}

func (rc *RabbitmqClient) ConsumeWithHandler(prefetchCnt int, queue, consumerTag string, handler AMQPMsgHandler, opts ...ConsumerOption) error {
	mqCh, err := rc.handleMQChInit()
	if err != nil {
		return err
	}
	ch, err := rc.consume(mqCh, prefetchCnt, queue, consumerTag, opts...)
	if err != nil {
		select {
		case <-rc.done:
			return ErrAmqpShutdown
		default:
			return rc.ConsumeWithHandler(prefetchCnt, queue, consumerTag, handler, opts...)
		}
	}

	rc.logger.Info("RabbitmqClient consuming...")
	for msg := range ch {
		handler(msg)
	}

	select {
	case <-rc.done:
		return ErrAmqpShutdown
	default:
		return rc.ConsumeWithHandler(prefetchCnt, queue, consumerTag, handler, opts...)
	}
}

func (rc *RabbitmqClient) consume(mqCh *amqp.Channel, prefetchCnt int, queue, consumerTag string, opts ...ConsumerOption) (ch <-chan amqp.Delivery, err error) {
	err = mqCh.Qos(prefetchCnt, 0, false)
	if err != nil {
		rc.logger.Debugf("RabbitmqClient.Consume: mqCh.Qos: %s", err)
		return nil, err
	}

	defaultOpts := consumerParams{
		consumerTag: uuid.NewString(),
		args:        amqp.Table{},
	}

	for i := range opts {
		opts[i](&defaultOpts)
	}

	ch, err = mqCh.Consume(queue, defaultOpts.consumerTag, defaultOpts.autoAck, defaultOpts.exclusive, false, defaultOpts.noWait, defaultOpts.args)
	if err != nil {
		rc.logger.Debugf("RabbitmqClient.Consume: mqCh.Consume: %s", err)
	}
	return
}

func (client *RabbitmqClient) pingpong() {
	tick := time.NewTicker(time.Second * 5)
	defer tick.Stop()
	for {
		select {
		case <-client.done:
			return
		case <-client.notifyChanClose:
			client.logger.Debug("RabbitmqClient chan closed")
			err := client.handleReConnSync(withReconnOptionsMQInit)
			if err != nil {
				return
			}
			continue
		case <-client.notifyConnClose:
			client.logger.Debug("RabbitmqClient conn closed")
			err := client.handleReConnSync(withReconnOptionsMQInit)
			if err != nil {
				return
			}
		case <-tick.C:
		}
	}
}

// handleReconnAsync
func (client *RabbitmqClient) handleReconnAsync() error {
	// 已开始重连
	if atomic.LoadUint32(&client.reConnFlag) == 1 {
		client.logger.Debug("handleReconnAsync reconnecting")
		return ErrAmqpReconn
	} else {
		go client.handleReConnSync(withReconnOptionsMQInit)
		return ErrAmqpReconn
	}
}

// handleReConnSync will block until conn successfully or conn timeout
func (client *RabbitmqClient) handleReConnSync(opts ...reconnOption) error {
	client.mu.Lock()
	if !atomic.CompareAndSwapUint32(&client.reConnFlag, 0, 1) {
		client.logger.Info("RabbitmqClient waiting for reconnecting...")
		client.mu.Unlock()
		select {
		case <-client.reconnDone:
			return nil
		case <-client.done:
			return ErrAmqpShutdown
		}
	}
	client.reconnDone = make(chan struct{}, 1)
	client.mu.Unlock()
	err := client.reconnWithBlock()
	if err == nil {
		for i := range opts {
			err = opts[i](client)
			if err != nil {
				return err
			}
		}
	}
	close(client.reconnDone)
	atomic.StoreUint32(&client.reConnFlag, 0)
	return err
}

func (client *RabbitmqClient) reconnWithBlock() error {
	var err error
	delay := time.Millisecond * 500
	client.logger.Info("RabbitmqClient attempt to connect")
loop:
	for {
		conn, err1 := client.makeConn()
		if err1 != nil {
			if delay > client.connTimeout {
				err = ErrAmqpConnTimeout
				close(client.done)
				break
			}
			select {
			case <-client.done:
				err = ErrAmqpShutdown
				break loop
			case <-time.After(delay):
				client.logger.Infof("RabbitmqClient connect error, Retrying after %s...", delay)
				delay *= 2
			}
			continue
		}
		client.chgConn(conn)

		select {
		case <-client.done:
			err = ErrAmqpShutdown
			break loop
		case <-client.notifyConnClose:
			client.logger.Info("RabbitmqClient connection closed. Reconnecting...")
		default:
			err = nil
			break loop
		}
	}

	if err == nil {
		client.logger.Info("RabbitmqClient connect successfully")
	} else {
		client.logger.Infof("RabbitmqClient connect error: %s", err)
	}

	return err
}

func (client *RabbitmqClient) handleMQChInit() (ch *amqp.Channel, err error) {
	delay := time.Second
	for {
		if !client.validateConn() {
			err = client.handleReConnSync()
			if err != nil {
				return nil, err
			}
		}
		ch, err := client.makeMQCh()
		if err != nil {
			if delay > client.initMQChTimeout {
				return nil, ErrAmqpChannelInitTimeout
			}
			client.logger.Infof("RabbitmqClient init channel error: %s. Retrying after %s...", err, delay)

			select {
			case <-client.done:
				return nil, ErrAmqpShutdown
			case <-time.After(delay):
				delay *= 2
			}
			continue
		}

		return ch, nil
	}
}

func (client *RabbitmqClient) makeConn() (*amqp.Connection, error) {
	if client.conn != nil && !client.conn.IsClosed() {
		return client.conn, nil
	}
	conn, err := amqp.Dial(client.url)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (client *RabbitmqClient) chgConn(conn *amqp.Connection) {
	client.conn = conn
	client.notifyConnClose = make(chan *amqp.Error, 1)
	client.conn.NotifyClose(client.notifyConnClose)
}

func (client *RabbitmqClient) validateConn() bool {
	if client.conn != nil && !client.conn.IsClosed() {
		return true
	} else {
		return false
	}
}

func (client *RabbitmqClient) makeMQCh() (*amqp.Channel, error) {
	if client.conn == nil {
		return nil, ErrAmqpConnNil
	}
	ch, err := client.conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.Confirm(false)
	if err != nil {
		return nil, err
	}

	return ch, nil
}

func (client *RabbitmqClient) chgDefaultCh(channel *amqp.Channel) {
	client.mqCh = channel
	client.notifyChanClose = make(chan *amqp.Error, 1)
	client.notifyConfirm = make(chan amqp.Confirmation, 1)
	client.mqCh.NotifyClose(client.notifyChanClose)
	client.mqCh.NotifyPublish(client.notifyConfirm)
}
