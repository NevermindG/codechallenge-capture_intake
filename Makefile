.PHONY: fmt vet test test-unit test-integration run compose-up compose-down

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

vet:
	go vet ./...

test: test-unit

test-unit:
	go test ./...

test-integration:
	DATABASE_URL=postgres://capture:capture@localhost:5432/captures?sslmode=disable go test -tags=integration ./internal/capture/adapters/postgres ./internal/capture/application/outbox -count=1

run:
	go run ./cmd/api

compose-up:
	docker compose up --build

compose-down:
	docker compose down -v
