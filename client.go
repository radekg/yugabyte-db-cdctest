package main

import (
	"github.com/hashicorp/go-hclog"
	"github.com/radekg/yugabyte-db-go-client/client"
	ybApi "github.com/radekg/yugabyte-db-go-proto/v2/yb/api"
)

type clientProvider struct {
	hostPorts []*ybApi.HostPortPB
}

func newClientProvider(hps []*ybApi.HostPortPB) *clientProvider {
	return &clientProvider{
		hostPorts: hps,
	}
}

func (p *clientProvider) getClient(logger hclog.Logger) (client.YBConnectedClient, error) {
	return getSingleNodeClient(p.hostPorts, logger)
}
