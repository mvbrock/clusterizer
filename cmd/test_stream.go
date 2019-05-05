package main

import "flag"
import "fmt"
import "context"
import "log"
import kafka "github.com/segmentio/kafka-go"

func main() {
    kafkaHost := flag.String("kafkaHost", "localhost:9092", "The hostname:port string for the Kafka queue")
    kafkaTopic := flag.String("kafkaTopic", "default", "The Kafa topic to read messages from")

    flag.Parse()

    fmt.Println("kafkaHost: ", *kafkaHost)
    fmt.Println("kafkaTopic: ", *kafkaTopic)

    kafkaWriterConfig := kafka.WriterConfig{
        Brokers: []string{*kafkaHost},
        Topic: *kafkaTopic,
        Balancer: &kafka.LeastBytes{},
    }
    kafkaWriter := kafka.NewWriter(kafkaWriterConfig)

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
