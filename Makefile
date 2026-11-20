.PHONY: db-up test test-short

db-up:
	docker compose up -d --wait db

test:
	ROWAN_TEST_DATABASE_URL=postgres://rowan:rowan@localhost:54329/rowan?sslmode=disable go test ./... -race -count=1

test-short:
	go test ./... -short
