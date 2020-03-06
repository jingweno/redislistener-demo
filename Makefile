build:
	mkdir -p bin
	GOBIN=$(CURDIR)/bin go install ./cmd/...

request:
	ADDR=redis-channel REDIS_URL=$$(heroku config:get REDIS_URL) ./bin/client POST /test hi
