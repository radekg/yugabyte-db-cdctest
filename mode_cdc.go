package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/radekg/yugabyte-db-go-client/client"
	"github.com/radekg/yugabyte-db-go-client/utils/ybdbid"
	ybApi "github.com/radekg/yugabyte-db-go-proto/v2/yb/api"
)

func executeCDC(ybdbClient client.YBClient,
	logger hclog.Logger,
	loggerClient hclog.Logger,
	cfg *cdcConfig) int {

	var table *ybApi.ListTablesResponsePB_TableInfo
	response, err := listTables(ybdbClient, cfg.database)
	if err != nil {
		logger.Error("failed listing tables", "reason", err)
		return 1
	}

	for _, tableInfo := range response.Tables {
		if *tableInfo.Name == cfg.table {
			table = tableInfo
			break
		}
	}

	if table == nil {
		logger.Error("table not found")
		return 1
	}

	if err := waitForTableCreateDone(ybdbClient, table.Id); err != nil {
		logger.Error("failed while waiting for table create done status", "reason", err)
		return 1
	}

	logger.Info("table found")

	hostPorts, err := listHostPorts(ybdbClient)
	if err != nil {
		logger.Error("error while listing tablet servers", "reason", err)
		return 1
	}

	if len(hostPorts) == 0 {
		logger.Error("could not discover any hosts to run the CDC against")
		return 1
	}

	logger.Info("found host ports", "host-ports", hostPorts)

	tabletLocations, err := listTabletLocations(ybdbClient, table.Id)
	if err != nil {
		logger.Error("error while listing tablet locations", "reason", err)
		return 1
	}

	if len(tabletLocations) == 0 {
		logger.Error("no tablet locations to run the CDC against")
		return 1
	}

	logger.Info("found tablet locations", "num-tablet-locations", len(tabletLocations))

	cp := newClientProvider(hostPorts)

	var streamIDBytes []byte
	if cfg.stream == "" {
		for {

			c, err := cp.getClient(loggerClient)
			if err != nil {
				logger.Error("could not get connected client, going to retry", "reason", err)
				<-time.After(time.Millisecond * 100)
				continue
			}

			streamResponse, err := createCDCStream(c, table.Id)
			if err != nil {
				c.Close()
				logger.Error("error creating new CDC stream, going to retry", "reason", err)
				<-time.After(time.Millisecond * 100)
				continue
			}
			c.Close()

			streamIDBytes = streamResponse.StreamId
			break

		}
		parsedStreamID, err := ybdbid.TryParseFromBytes(streamIDBytes)
		if err != nil {
			logger.Error("failed parsing new stream ID", "reason", err)
			return 1
		}
		logger = logger.With("stream-id", parsedStreamID.String())
		logger.Info("created a new CDC stream")
	} else {
		reverseParsedStreamID, err := ybdbid.TryParseFromString(cfg.stream)
		if err != nil {
			logger.Error("failed parsing given CDC stream ID to bytes", "reason", err)
			return 1
		}
		streamIDBytes = reverseParsedStreamID.Bytes()
		_, getStreamErr := getCDCStreamByID(ybdbClient, streamIDBytes)
		if getStreamErr != nil {
			logger.Error("failed fetching stream info for given stream ID",
				"reason", getStreamErr)
			return 1
		}
	}

	// handle shutdown gracefully:
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		logger.Info("shutting down...")
		cancelFunc()
	}()

	// start consuming:
	wg := &sync.WaitGroup{}
	for _, location := range tabletLocations {
		wg.Add(1)
		go func(tabletID []byte) {
			consume(ctx, logger, loggerClient, cp, streamIDBytes, tabletID)
			wg.Done()
		}(location.TabletId)
	}
	wg.Wait()

	logger.Info("all work done, bye...")

	return 0

}
