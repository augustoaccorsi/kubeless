package main

import (
	"os"
	"os/signal"

	natsio "github.com/nats-io/go-nats"
	"github.com/sirupsen/logrus"
)

func main() {
	url := os.Getenv("URL")
	if url == "" {
		logrus.Fatalf("URL can't be empty. Please set env URL.")
	}
	topic := os.Getenv("TOPIC")
	if topic == "" {
		logrus.Fatalf("TOPIC can't be empty. Please set env TOPIC.")
	}

	natsConnection, err := natsio.Connect(url)
	if err != nil {
		logrus.Fatalf("Can't connect to NATS server %s: %v", url, err)
	}

	messageChan := make(chan *natsio.Msg, 64)
	_, err = natsConnection.ChanSubscribe(topic, messageChan)
	if err != nil {
		logrus.Fatalf("Can't subscribe to topic %s: %v", topic, err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case natsMessage := <-messageChan:
				logrus.Fatalf("Received messages: ", natsMessage)
			case <-signals:
				logrus.Fatalf("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
}
