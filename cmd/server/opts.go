package main

import (
	"strings"
)

var opts struct {
	RestAPI struct {
		Enabled  bool   `long:"enabled" description:"enable restapi server" env:"ENABLED"`
		BindAddr string `long:"bind-addr" description:"address to bind restapi server" env:"BIND_ADDR" default:":8000"`
	} `group:"restapi" namespace:"restapi" env-namespace:"RESTAPI"`
	GRPC struct {
		BindAddr   string `long:"bind-addr" description:"address to bind grpc server" env:"BIND_ADDR" default:":3000"`
		LocalAddr  string `long:"local-addr" description:"address to connect to local grpc server" env:"LOCAL_ADDR" default:"127.0.0.1:3000"`
		PublicAddr string `long:"public-addr" description:"address to advertise to other nodes" env:"PUBLIC_ADDR" required:"true"`
	} `group:"grpc" namespace:"grpc" env-namespace:"GRPC"`

	Verbose bool `long:"verbose" description:"verbose mode" env:"VERBOSE"`
}

func parseAddrs(addrs string) []string {
	sl := strings.Split(addrs, ",")
	res := make([]string, 0, len(sl))

	for _, addr := range sl {
		trimmed := strings.TrimSpace(addr)
		if trimmed != "" {
			res = append(res, trimmed)
		}
	}

	return res
}
