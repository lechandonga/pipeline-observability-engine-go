.PHONY: build test race vet fmt demo clean

build:
	go build ./...

test:
	go test -count=1 ./...

race:
	CGO_ENABLED=1 go test -race -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -d .

demo:
	go run ./cmd/poe

clean:
	rm -rf /tmp/poe-demo-data
