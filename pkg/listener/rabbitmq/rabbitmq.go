package main

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

type rabbitMq struct {
	brokerUrl                  string
	ExchangeName               string
	QueueName                  string
	brokerConn                 *amqp.Connection
	brokerChannel              *amqp.Channel
	brokerQueue                amqp.Queue
	brokerInputMessagesChannel <-chan amqp.Delivery
}

func main() {
	rmq := &rabbitMq{}
	rmq.brokerUrl = os.Getenv("BROKER_URL")
	if rmq.brokerUrl == "" {
		logrus.Fatalf("BROKER_URL can't be empty. Please set env BROKER_URL.")
	}
	rmq.QueueName = os.Getenv("BROKER_QUEUE")
	if rmq.QueueName == "" {
		logrus.Fatalf("BROKER_QUEUE can't be empty. Please set env BROKER_QUEUE.")
	}
	rmq.ExchangeName = os.Getenv("BROKER_EXCHANGE")
	if rmq.ExchangeName == "" {
		logrus.Fatalf("BROKER_EXCHANGE can't be empty. Please set env BROKER_EXCHANGE.")
	}

	if err := rmq.createBrokerResources(); err != nil {
		logrus.Fatalf("Failed to create broker resources: %v", err)
	}

	// start listening for published messages
	go rmq.handleBrokerMessages()
}

func (rmq *rabbitMq) createBrokerResources() error {
	var err error

	//Connect to broker
	rmq.brokerConn, err = amqp.Dial(rmq.brokerUrl)
	if err != nil {
		return fmt.Errorf("Failed to create connection to broker: %v", err)
	}

	//Create broker channel
	rmq.brokerChannel, err = rmq.brokerConn.Channel()
	if err != nil {
		return fmt.Errorf("Failed to create channel: %v", err)
	}

	// create the exchange
	err = rmq.brokerChannel.ExchangeDeclare(rmq.ExchangeName,
		"topic",
		false,
		false,
		false,
		false,
		nil)
	if err != nil {
		return fmt.Errorf("Failed to declare exchange: %v", err)
	}

	// declare queue
	rmq.brokerQueue, err = rmq.brokerChannel.QueueDeclare(
		rmq.QueueName, // queue name (account  + function name)
		false,         // durable  TBD: change to true if/when we bind to persistent storage
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		return fmt.Errorf("Failed to declare queue: %v", err)
	}

	// bind to queue
	err = rmq.brokerChannel.QueueBind(
		rmq.brokerQueue.Name, // queue name
		"*",                  // routing key
		rmq.ExchangeName,     // exchange
		false,
		nil)
	if err != nil {
		return fmt.Errorf("Failed to bind to queue: %v", err)
	}

	// start consuming from queue
	rmq.brokerInputMessagesChannel, err = rmq.brokerChannel.Consume(
		rmq.brokerQueue.Name, // queue
		"",                   // consumer
		false,                // auto-ack
		false,                // exclusive
		false,                // no-local
		true,                 // no-wait
		nil,                  // args
	)
	if err != nil {
		return fmt.Errorf("Failed to start consuming messages: %v", err)
	}

	return nil
}

func (rmq *rabbitMq) handleBrokerMessages() {
	for message := range rmq.brokerInputMessagesChannel {
		fmt.Println(message)
	}
}
