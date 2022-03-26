package main

import "flag"
import "fmt"
import "context"
import "log"
import kafka "github.com/segmentio/kafka-go"
import serf "github.com/hashicorp/serf/serf"

func main() {
    // Kafka options
    kafkaHost := flag.String("kafkaHost", "localhost:9092", "The hostname:port string for the Kafka queue")
    kafkaTopic := flag.String("kafkaTopic", "default", "The Kafa topic to read messages from")
    kafkaConsumerGroup := flag.String("kakfaConsumerGroup", "default", "The Kafka consumer group")

    // Serf options
    serfBind := flag.String("serfBind", "", "The hostname:port for binding the Serf agent")
    serfConnect := flag.String("serfConnect", "", "The hostname:port of an instance to join")
    serfNodeName := flag.String("serfNodeName", "0", "The unique node name of this instance")

    // KMeans options
    numClusters := flag.Int("numClusters", 1, "The number of clusters to track")
    batchSize := flag.Int("batchSize", 100, "The batch size of entering and exiting data")
    windowSize := flag.Int("windowSize", 10, "The window size in number of batches")

    // Parse the command line options
    flag.Parse()

    // Print the command line options
    fmt.Println("kafkaHost: ", *kafkaHost)
    fmt.Println("kafkaTopic: ", *kafkaTopic)
    fmt.Println("kafkaConsumerGroup: ", *kafkaConsumerGroup)
    fmt.Println("serfBind: ", *serfBind)
    fmt.Println("serfConnect: ", *serfConnect)
    fmt.Println("serfNodeName: ", *serfNodeName)
    fmt.Println("numClusters: ", *numClusters)
    fmt.Println("batchSize: ", *batchSize)
    fmt.Println("windowSize: ", *windowSize)

    // Establish the Serf configuration
    serfConfig := serf.DefaultConfig()
    serfConfig.NodeName = *serfNodeName
    if *serfBind != "" {
        serfConfig.MemberlistConfig.BindAddr = "localhost"
        serfConfig.MemberlistConfig.BindPort = 7947
    }
    serfAgent, err := serf.Create(serfConfig)
    if err != nil {
        log.Fatalln(err)
    }
    if *serfConnect != "" {
        serfAgent.Join([]string{*serfConnect}, false)
    }

    // Print out the member list
    members := serfAgent.Members()
    for _, member := range members {
        fmt.Println(member)
    }

    // Establish the Kafka reader
    kafkaReaderConfig := kafka.ReaderConfig{
        Brokers: []string{*kafkaHost},
        GroupID: *kafkaConsumerGroup,
        Topic: *kafkaTopic,
        MinBytes: 0,
        MaxBytes: 10e7, // 100MB
    }
    kafkaReader := kafka.NewReader(kafkaReaderConfig)

    // Read messages off the queue
    for {
        msg, err := kafkaReader.ReadMessage(context.Background())
        if err != nil {
            log.Println(err)
        }
        fmt.Printf("message at topic:%v partition:%v offset:%v  %s = %s\n",
            msg.Topic, msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
    }
}
