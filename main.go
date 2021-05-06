package main

import (
	"fmt"
	"log"
	"os"

	"github.com/Shopify/sarama"
)

const (
	kafkaConn = "localhost:9092"
	topic     = "kafka-headers"
)

func main() {
	fmt.Println("### Kafka Producer ###")
	//create producer
	producer, err := initProducer()
	if err != nil {
		fmt.Println("Error producer: ", err.Error())
		os.Exit(1)
	}

	headers := make(map[string]string, 0)
	headers["header_1"] = "content_1"
	msg := "Kafka tutorial"

	produce(msg, headers, producer)
}

func initProducer() (sarama.SyncProducer, error) {
	//setup sarama log to stdout
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

	//producer config
	config := sarama.NewConfig()
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Version = sarama.V0_11_0_0

	//create producer
	prd, err := sarama.NewSyncProducer([]string{kafkaConn}, config)

	return prd, err
}

func produce(message string, headers map[string]string, producer sarama.SyncProducer) {
	// publish sync
	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.StringEncoder(message),
		Headers: convertHeaders(headers),
	}
	p, o, err := producer.SendMessage(msg)
	if err != nil {
		fmt.Println("Error publish: ", err.Error())
	}

	fmt.Println("Partition: ", p)
	fmt.Println("Offset: ", o)
}

func convertHeaders(headers map[string]string) []sarama.RecordHeader {
	output := make([]sarama.RecordHeader, 0)
	for key, value := range headers {
		output = append(output, sarama.RecordHeader{
			Key:   []byte(key),
			Value: []byte(value),
		})
	}
	return output
}
