package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/jingweno/upterm/redislistener"
)

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		log.Fatal("missing env var ADDR")
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		log.Fatal("missing env var REDIS_URL")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	opt := redislistener.RedisOpt{
		Network: "tcp",
		Address: redisURL,
	}
	dialer, err := redislistener.NewDialer(ctx, addr, opt)
	if err != nil {
		log.Fatal(err)
	}
	defer dialer.Close()

	tr := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialer.DialContext(ctx)
		},
	}

	client := &http.Client{Transport: tr}

	var (
		method string
		path   string
		body   string
	)

	if len(os.Args) >= 2 {
		method = os.Args[1]
	}

	if len(os.Args) >= 3 {
		path = os.Args[2]
	}

	if len(os.Args) >= 4 {
		body = os.Args[3]
	}

	if method == "" {
		method = "GET"
	}

	url := fmt.Sprintf("http://%s%s", addr, path)
	req, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		log.Fatal(err)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("%s\n", b)
}
