package main

import (
	"context"
	"crypto/hmac"
	"crypto/md5"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
)

var (
	clientId = "faas"

	defaultAMQPClient *RabbitmqClient
	onceAmqp          sync.Once
)

func getAMQPURLFromEnv(host, port, key, secret string) string {
	u, p := getAMQPAccess(key, secret)
	return fmt.Sprintf("amqp://%s:%s@%s:%s", u, p, host, port)
}

func getAMQPAccess(key, secret string) (username, password string) {
	timestamp := strconv.Itoa(int(time.Now().Unix()))
	sign, _ := authAMQPSign(clientId, key, timestamp, secret, "hmacsha1")
	username = fmt.Sprintf("%s&%s&%s", clientId, key, timestamp)
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

func InitAMQPConsumer(ctx context.Context, host, port, key, secret string) *RabbitmqClient {
	onceAmqp.Do(func() {
		reconnCtx, cancel := context.WithCancel(ctx)
		defaultAMQPClient = &RabbitmqClient{
			url:          getAMQPURLFromEnv(host, port, key, secret),
			cancelReconn: cancel,
			reconnCtx:    reconnCtx,
			closeCh:      make(chan struct{}),
		}
		log.Println("amqp connecting")
		err := defaultAMQPClient.initConn()
		if err != nil {
			log.Fatalf("amqp init error: %s", err)
		}
	})
	log.Println("amqp connected")
	return defaultAMQPClient
}

type RabbitmqClient struct {
	url           string
	accessKey     string
	conn          *amqp.Connection
	mqCh          *amqp.Channel
	reConnFlag    uint32
	currReConnNum int

	mu           sync.Mutex
	closeCh      chan struct{}
	cancelReconn context.CancelFunc
	reconnCtx    context.Context
}

func (rc *RabbitmqClient) initConn() (err error) {
	rc.conn, err = amqp.Dial(rc.url)
	if err != nil {
		return
	}
	rc.mqCh, err = rc.conn.Channel()
	return
}

func (rc *RabbitmqClient) reConn() error {
	var err error
	rc.mu.Lock()
	if !atomic.CompareAndSwapUint32(&rc.reConnFlag, 0, 1) {
		rc.mu.Unlock()
		<-rc.reconnCtx.Done()
		return nil
	}
	rc.reconnCtx, rc.cancelReconn = context.WithCancel(context.Background())
	rc.mu.Unlock()
	for {
		if rc.conn == nil || rc.conn.IsClosed() {
			log.Printf("amqp %dth reconnecting ...", rc.currReConnNum+1)
			err = rc.initConn()
			if err != nil {
				rc.currReConnNum++
				time.Sleep(time.Second * 10)
				continue
			}
		}
		rc.currReConnNum = 0
		log.Println("amqp reconnected successfully")
		atomic.StoreUint32(&rc.reConnFlag, 0)
		rc.cancelReconn()
		return nil
	}
}

func (rc *RabbitmqClient) Consume(prefetchCnt int, queue, consumerName string) (ch <-chan amqp.Delivery, err error) {
	err = rc.mqCh.Qos(prefetchCnt, 0, false)
	if err != nil {
		err = rc.reConn()
	}
	if err != nil {
		return
	}
	ch, err = rc.mqCh.Consume(queue, consumerName, true, false, false, true, amqp.Table{})
	if err != nil {
		err = rc.reConn()
	}
	return
}

func (rc *RabbitmqClient) Close() error {
	close(rc.closeCh)
	if rc.conn != nil {
		return rc.conn.Close()
	} else {
		return nil
	}
}

func (rc *RabbitmqClient) getClientid() string {
	return clientId + "-" + generateUUID(8)
}

func (rc *RabbitmqClient) ConsumeWithHanlder(ctx context.Context, handler func([]byte)) {
	ch, err := rc.Consume(100, rc.accessKey, rc.getClientid())
	if err != nil {
		log.Printf("Consume: %s", err)
		return
	}
	log.Println("amqp consuming...")
	for msg := range ch {
		go handler(msg.Body)
	}

	select {
	case <-ctx.Done():
		return
	default:
		go rc.ConsumeWithHanlder(ctx, handler)
	}
}

func generateUUID(maxLen int) string {
	raw := strings.ReplaceAll(uuid.NewString(), "-", "")
	if len(raw) <= maxLen {
		return raw
	} else {
		return raw[:maxLen]
	}
}
