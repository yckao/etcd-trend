.PHONY: build test vet demo clean

build:
	mkdir -p bin
	go build -o bin/recorder ./cmd/recorder
	go build -o bin/analyzer ./cmd/analyzer
	go build -o bin/etcd-trend-demo ./cmd/etcd-trend-demo
	go build -o bin/etcd-trend-compat ./cmd/etcd-trend-compat
	go build -o bin/etcd-trend-loadtest ./cmd/etcd-trend-loadtest
	go build -o bin/etcd-trend-inspect ./cmd/etcd-trend-inspect
	go build -o bin/etcd-trend-web ./cmd/etcd-trend-web
	go build -o bin/etcd-trend-tui ./cmd/etcd-trend-tui

test:
	go test ./...

vet:
	go vet ./...

demo:
	go run ./cmd/etcd-trend-demo

clean:
	rm -f bin/recorder bin/analyzer bin/etcd-trend-demo bin/etcd-trend-compat bin/etcd-trend-loadtest bin/etcd-trend-inspect bin/etcd-trend-web bin/etcd-trend-tui
