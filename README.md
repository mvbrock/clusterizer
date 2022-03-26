# Clusterizer

A scalable streaming cluster analysis command line tool. It pulls data off of a Kafka topic and puts cluster centers onto another Kafka topic. Scaling the throughput is accomplished by connecting more workers to the initial worker using [Serf](https://www.serf.io/).
