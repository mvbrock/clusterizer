all: go-get build

go-get:
	go get github.com/hashicorp/serf/client
	go get github.com/hashicorp/serf/serf
	go get github.com/segmentio/kafka-go

build:
	mkdir -p bin/
	go build -o bin/ssb-skmeans cmd/ssb_skmeans.go
	go build -o bin/test-stream cmd/test_stream.go

clean:
	rm bin/*
