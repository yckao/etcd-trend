.PHONY: build install test vet demo clean

build:
	mkdir -p bin
	go build -o bin/etcd-trend .

install:
	go install .

test:
	go test ./...

vet:
	go vet ./...

demo:
	go run . demo

clean:
	rm -f bin/etcd-trend bin/recorder bin/analyzer bin/etcd-trend-demo bin/etcd-trend-compat bin/etcd-trend-loadtest bin/etcd-trend-inspect bin/etcd-trend-web bin/etcd-trend-tui
