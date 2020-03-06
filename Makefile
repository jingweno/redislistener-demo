build:
	mkdir -p bin
	GOBIN=$(CURDIR)/bin go install ./cmd/...

serve:
	HOST=redis-channel REDIS_URL=$$(heroku config:get REDIS_URL) ./bin/server

request:
	HOST=redis-channel REDIS_URL=$$(heroku config:get REDIS_URL) ./bin/client POST /test $$(date +"%T")
