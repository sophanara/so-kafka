build: 
	@go build -o bin/kafka . 

run: build 
	@./bin/kafka
