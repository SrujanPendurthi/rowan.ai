.PHONY: db-up test test-go test-bff test-short

db-up:
	docker compose up -d --wait db

test: test-go test-bff

test-go:
	ROWAN_TEST_DATABASE_URL=postgres://rowan:rowan@localhost:54329/rowan?sslmode=disable go test ./... -race -count=1

test-bff:
	cd apps/bff && npm ci && npm run typecheck && ROWAN_TEST_DATABASE_URL=postgres://rowan:rowan@localhost:54329/rowan?sslmode=disable npm test

test-short:
	go test ./... -short
