package main

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"os"

	"github.com/jingweno/upterm/redislistener"
)

func main() {
	addr := os.Getenv("ADDR")
	if addr == "" {
		log.Fatal("missing env var ADDR")
	}

	ln, err := redislistener.NewListener(ctx, addr, opt)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			dump, err := httputil.DumpRequest(r, true)
			if err != nil {
				http.Error(w, fmt.Sprint(err), http.StatusInternalServerError)
				return
			}

			log.Printf("%s", dump)
			fmt.Fprintf(w, "%q", dump)
		}),
	}
	defer srv.Close()

	_ = srv.Serve(ln)
}
