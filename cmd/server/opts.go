package main

import (
	"strings"
)

var opts struct {
	Node struct {
		ID   uint32 `long:"id" env:"ID" required:"true" description:"unique node id"`
		Name string `long:"name" env:"NAME" required:"true" description:"node name"`
	} `group:"node" namespace:"node" env-namespace:"NODE"`

	RestAPI struct {
		Enabled  bool   `long:"enabled" description:"enable restapi server" env:"ENABLED"`
		BindAddr string `long:"bind-addr" description:"address to bind restapi server" env:"BIND_ADDR" default:":8000"`
	} `group:"restapi" namespace:"restapi" env-namespace:"RESTAPI"`
	GRPC struct {
		BindAddr   string `long:"bind-addr" description:"address to bind grpc server" env:"BIND_ADDR" default:":3000"`
		LocalAddr  string `long:"local-addr" description:"address to connect to local grpc server" env:"LOCAL_ADDR" default:"127.0.0.1:3000"`
		PublicAddr string `long:"public-addr" description:"address to advertise to other nodes" env:"PUBLIC_ADDR" required:"true"`
	} `group:"grpc" namespace:"grpc" env-namespace:"GRPC"`
	Cluster struct {
		JoinAddrs          string `long:"join-addrs" description:"comma-separated list of nodes to join" env:"JOIN_ADDRS"`
		ProbeTimeout       int    `long:"probe-timeout" description:"failure detection timeout (ms)" env:"PROBE_TIMEOUT" default:"5000"`
		ProbeInterval      int    `long:"probe-interval" description:"failure detection interval (ms)" env:"PROBE_INTERVAL" default:"1000"`
		ProbeIndirectNodes int    `long:"probe-indirect-nodes" description:"number nodes for indirect probe" env:"PROBE_INDIRECT_NODES" default:"1"`
	} `group:"cluster" namespace:"cluster" env-namespace:"CLUSTER"`

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
