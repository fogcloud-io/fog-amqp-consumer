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

type Delivery = amqp.Delivery

var (
	ErrAmqpShutdown           = errors.New("amqp shutdown")
	ErrAmqpChannelInitTimeout = errors.New("amqp channel init timeout")
	ErrAmqpConnTimeout        = errors.New("amqp connect timeout")
	ErrAmqpConnNil            = errors.New("amqp conn nil")
	ErrAmqpReconn             = errors.New("amqp reconnecting")
)

type ClientOption func(*RabbitmqClient)

type AMQPMsgHandler = func(amqp.Delivery)

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
		connTimeout:     time.Second * 30,
		initMQChTimeout: time.Second * 30,
		logger:          l.Sugar(),
	}

	for _, opt := range opts {
		opt(cli)
	}

	err := cli.handleReConnSync()
	if err != nil {
		return nil, err
	}
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

func (rc *RabbitmqClient) Consume(prefetchCnt int, queue, consumerName string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (ch <-chan amqp.Delivery, err error) {
	err = rc.mqCh.Qos(prefetchCnt, 0, false)
	if err != nil {
		rc.logger.Debugf("RabbitmqClient.Consume: mqCh.Qos: %s", err)
		rc.handleReConnSync()
	}
	if err != nil {
		return
	}
	ch, err = rc.mqCh.Consume(queue, consumerName, autoAck, exclusive, noLocal, noWait, args)
	if err != nil {
		rc.logger.Debugf("RabbitmqClient.Consume: mqCh.Consume: %s", err)
		err = rc.handleReConnSync()
	}
	return
}

// ConsumeWithHandler will consume with block until client closed or connect timeout
func (rc *RabbitmqClient) ConsumeWithHandler(prefetchCnt int, queue, consumerName string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table, handler AMQPMsgHandler) error {
	ch, err := rc.Consume(prefetchCnt, queue, consumerName, autoAck, exclusive, noLocal, noWait, args)
	if err != nil {
		rc.logger.Infof("RabbitmqClient Consume: %s", err)
		return err
	}
	rc.logger.Info("RabbitmqClient consuming...")
	for msg := range ch {
		handler(msg)
	}

	select {
	case <-rc.done:
		return ErrAmqpShutdown
	default:
		err = rc.handleReConnSync()
		if err != nil {
			return err
		}
		return rc.ConsumeWithHandler(prefetchCnt, queue, consumerName, autoAck, exclusive, noLocal, noWait, args, handler)
	}
}

// handleReconnAsync
func (client *RabbitmqClient) handleReconnAsync() error {
	// 已开始重连
	if atomic.LoadUint32(&client.reConnFlag) == 1 {
		client.logger.Debug("handleReconnAsync reconnecting")
		return ErrAmqpReconn
	} else {
		go client.handleReConnSync()
		return ErrAmqpReconn
	}
}

// handleReConnSync will block until conn successfully or conn timeout
func (client *RabbitmqClient) handleReConnSync() error {
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
	close(client.reconnDone)
	atomic.StoreUint32(&client.reConnFlag, 0)
	return err
}

func (client *RabbitmqClient) reconnWithBlock() error {
	var err error
	delay := time.Second
	client.logger.Info("RabbitmqClient attempt to connect")
loop:
	for {
		conn, err1 := client.makeConn()
		if err1 != nil {
			if delay > client.connTimeout {
				err = ErrAmqpConnTimeout
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

		if err = client.handleMQChInit(); err != nil {
			break
		}

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

func (client *RabbitmqClient) handleMQChInit() (err error) {
	delay := time.Second
	for {
		client.logger.Info("RabbitmqClient attempt to init channel")
		if !client.validateConn() {
			err = client.handleReConnSync()
			if err != nil {
				return err
			}
		}
		ch, err := client.makeMQCh()
		if err != nil {
			if delay > client.initMQChTimeout {
				return ErrAmqpChannelInitTimeout
			}
			client.logger.Infof("RabbitmqClient init channel error: %s. Retrying after %s...", err, delay)

			select {
			case <-client.done:
				return ErrAmqpShutdown
			case <-time.After(delay):
				delay *= 2
			}
			continue
		}

		client.chgMQCh(ch)
		select {
		case <-client.done:
			return ErrAmqpShutdown
		case <-client.notifyChanClose:
			client.logger.Info("RabbitmqClient channel closed. Re-running init...")
		default:
			return nil
		}
	}
}

func (client *RabbitmqClient) makeConn() (*amqp.Connection, error) {
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

func (client *RabbitmqClient) chgMQCh(channel *amqp.Channel) {
	client.mqCh = channel
	client.notifyChanClose = make(chan *amqp.Error, 1)
	client.notifyConfirm = make(chan amqp.Confirmation, 1)
	client.mqCh.NotifyClose(client.notifyChanClose)
	client.mqCh.NotifyPublish(client.notifyConfirm)
}
