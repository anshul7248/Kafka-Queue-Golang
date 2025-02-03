package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "logs"

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Error creating kafka producer: %v", err)
	}
	defer producer.Close()

	for i := 0; i < 10; i++ {
		logMessage := fmt.Sprintf("Log message %d", i)

		// Create a Kafka message
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(logMessage),
		}
		// time.Sleep(2 * time.Now())
		// Send the message to Kafka
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Printf("Failed to send message: %v", err)
		} else {
			fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
		}
	}

}

// func producerFunction() {
// 	logMessage := fmt.Println("Log message")
// }
