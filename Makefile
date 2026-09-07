.PHONY: fmt test integration-test run-api run-worker run-payments compose-up compose-down

TEST_DATABASE_URL ?= postgres://durable:durable@localhost:5432/durable?sslmode=disable

fmt:
	gofmt -w ./cmd ./internal ./examples

test:
	go test ./...

integration-test:
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" go test ./internal/store/postgres -run Integration -count=1

run-api:
	go run ./cmd/workrail api

run-worker:
	go run ./cmd/workrail worker

# The payments example: HTTP API on :8090 plus a worker on the payouts queue.
# Run `make run-api` alongside it for the dashboard. See examples/payments.
run-payments:
	go run ./examples/payments

compose-up:
	docker compose up --build

compose-down:
	docker compose down -v
