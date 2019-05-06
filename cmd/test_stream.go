package main

import "flag"
import "fmt"
import "context"
import "log"
import kafka "github.com/segmentio/kafka-go"

func main() {
    kafkaHost := flag.String("kafkaHost", "localhost:9092", "The hostname:port string for the Kafka queue")
    kafkaTopicName := flag.String("kafkaTopicName", "default", "The Kafa topic to read messages from")
    kafkaTopicParts := flag.Int("kafkaTopicParts", 1, "The number of Kafka topic partitions")
    arraySize := flag.Int("arraySize", 10, "The number of random floating point values in each message's JSON array")

    flag.Parse()

    fmt.Println("kafkaHost: ", *kafkaHost)
    fmt.Println("kafkaTopicName: ", *kafkaTopicName)
    fmt.Println("kafkaTopicParts: ", *kafkaTopicParts)

    // Delete the topic and create a new one
    kafkaConn, _ := kafka.Dial("tcp", *kafkaHost)
    kafkaConn.DeleteTopics(*kafkaTopicName)
    kafkaTopicConfig := kafka.TopicConfig {
        Topic: *kafkaTopicName,
        NumPartitions: *kafkaTopicParts,
    }
    kafkaConn.CreateTopics(kafkaTopicConfig)
    kafkaConn.Close()

    // Create the writer
    kafkaWriterConfig := kafka.WriterConfig {
        Brokers: []string{*kafkaHost},
        Topic: *kafkaTopicName,
        Balancer: &kafka.LeastBytes{},
        Async: true,
    }
    kafkaWriter := kafka.NewWriter(kafkaWriterConfig)

    // Write the messages
    fmt.Println("Writing messages...")
    for i := 0; ; i++ {
        msg := kafka.Message{
            Key: []byte(fmt.Sprintf("%d", i)),
            Value: []byte(fmt.Sprint("0,1,2,3,4")),
        }
        err := kafkaWriter.WriteMessages(context.Background(), msg)
        if err != nil {
            log.Fatalln(err)
        }
    }
}
