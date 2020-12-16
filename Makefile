# Makefile template borrowed from https://sohlich.github.io/post/go_makefile/
# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOVERSION=1.15
BINARY_NAME="mqtt-influxdb-bridge"

build-all: test build-linux build-arm build-darwin build-raspi
test:
	$(GOTEST) -v ./...
clean:
	$(GOCLEAN)
	rm -f $(BINARY_NAME)

# Cross compilation
build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 $(GOBUILD) -o "bin/$(BINARY_NAME)_linux" -v "main.go"
build-arm:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm $(GOBUILD) -o "bin/$(BINARY_NAME)_arm" -v "main.go"
build-raspi:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm GOARM=5 $(GOBUILD) -o "bin/$(BINARY_NAME)_raspi" -v "main.go"
build-darwin:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 $(GOBUILD) -o "bin/$(BINARY_NAME)_darwin" -v "main.go"
