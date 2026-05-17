.PHONY: fmt test run-api run-worker compose-up compose-down

fmt:
	gofmt -w ./cmd ./internal

test:
	go test ./...

run-api:
	go run ./cmd/dwf api

run-worker:
	go run ./cmd/dwf worker

compose-up:
	docker compose up --build

compose-down:
	docker compose down -v

