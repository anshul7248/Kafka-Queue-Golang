package main

import (
	"fmt"
	"log"

	"github.com/IBM/sarama"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Log struct {
	ID      uint `gorm:"primaryKey"`
	Message string
}

var DB *gorm.DB

func InitDB() {
	dsn := "host=localhost user=postgres password=Anshul123 dbname=logs port=5432 sslmode=disable TimeZone=UTC"
	var err error
	DB, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}

	DB.AutoMigrate(&Log{})
}

func main() {
	InitDB()

	brokers := []string{"localhost:9092"}
	topic := "logs"

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(brokers, config)

	if err != nil {
		log.Fatalf("Error in creating kafka consumer, %v", err)
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetNewest)

	if err != nil {
		log.Fatalf("Error in consuming partition %v", err)
	}
	defer partitionConsumer.Close()

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			logMesaage := string(msg.Value)

			fmt.Printf("Received Log\n %s", logMesaage)
			DB.Create(&Log{Message: logMesaage})

		case err := <-partitionConsumer.Errors():
			log.Printf("Error consuming messages: %v", err)
		}
	}
}
