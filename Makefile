.PHONY: build test test-race vet fmt smoke up down verify clean

build:
	go build ./...

test:
	go test -count=1 ./...

test-race:
	go test -race -count=1 ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

up:
	docker compose up --build -d

down:
	docker compose down -v

# 对运行中的 Compose 栈执行端到端冒烟验收。
smoke:
	PROXY_URL=http://localhost:8080 ORIGIN_URL=http://localhost:8081 \
		sh ./scripts/accept.sh

# 一次性容器内完整自动测试（含竞态检测）。
verify:
	docker compose build verify
	docker compose run --rm verify

clean:
	docker compose down -v
