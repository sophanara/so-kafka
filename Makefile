build: 
	@go build -o bin/kafka ./cmd/server/main.go
	@go build -o bin/kafka-client ./cmd/client/main.go

run: build 
	@./bin/kafka

run-client: build
	@./bin/kafka-client
