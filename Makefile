.PHONY: fmt test integration-test run-api run-worker compose-up compose-down

TEST_DATABASE_URL ?= postgres://durable:durable@localhost:5432/durable?sslmode=disable

fmt:
	gofmt -w ./cmd ./internal

test:
	go test ./...

integration-test:
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test ./internal/store/postgres -run Integration -count=1

run-api:
	go run ./cmd/workrail api

run-worker:
	go run ./cmd/workrail worker

compose-up:
	docker compose up --build

compose-down:
	docker compose down -v
