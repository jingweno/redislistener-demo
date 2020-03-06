# redislistener-demo

This is a demo of the implementation of Golang's [`net.Listener`](https://golang.org/pkg/net/#Listener) and [`net.Conn`](https://golang.org/pkg/net/#Conn) with [Redis pub/sub](https://redis.io/topics/pubsub).
The source of the implementation is [here](https://github.com/jingweno/upterm/compare/redislistener).

## Demo

The demo serves a HTTP server on the Redis `net.Listener`. HTTP clients connect to the server via Redis pub/sub.

```
HTTP Client (1...*) <-> Redis <-> HTTP Server (1...*)
```

## Usage

This helps a client discover the server that does not have an external hostname.
The corresponding Redis pub/sub channel is the server hostname.
