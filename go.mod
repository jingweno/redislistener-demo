// +heroku install ./cmd/...
// +heroku goVersion 1.14

module github.com/jingweno/redislistener_server

go 1.14

require github.com/jingweno/upterm v0.0.9

replace github.com/jingweno/upterm => ../upterm
