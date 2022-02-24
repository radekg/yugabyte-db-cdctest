package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/radekg/yugabyte-db-go-client/client"
	"github.com/radekg/yugabyte-db-go-client/configs"
	"github.com/radekg/yugabyte-db-go-client/errors"
	"github.com/radekg/yugabyte-db-go-client/utils/ybdbid"
	ybApi "github.com/radekg/yugabyte-db-go-proto/v2/yb/api"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {

	cfg := &cdcConfig{}

	flag.StringVar(&cfg.database, "database", "", "database to use")
	flag.BoolVar(&cfg.logAsJSON, "log-as-json", false, "log as JSON")
	flag.StringVar(&cfg.logLevel, "log-level", defaultLogLevel, "log level")
	flag.StringVar(&cfg.logLevelClient, "log-level-client", defaultLogLevel, "YugabyteDB client log level")
	flag.StringVar(&cfg.masters, "masters", "127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102", "comma-delimited list of master addresses")
	flag.StringVar(&cfg.stream, "stream-id", "", "stream ID")
	flag.StringVar(&cfg.table, "table", "", "table to use")
	flag.Parse()

	os.Exit(process(cfg))

}

func process(cfg *cdcConfig) int {

	logger := hclog.New(&hclog.LoggerOptions{
		Name:       "cdctest",
		Level:      hclog.LevelFromString(cfg.logLevel),
		JSONFormat: cfg.logAsJSON,
	}).With("table", cfg.table)

	loggerClient := hclog.New(&hclog.LoggerOptions{
		Name:       "cdctest-client",
		Level:      hclog.LevelFromString(cfg.logLevelClient),
		JSONFormat: cfg.logAsJSON,
	}).With("table", cfg.table)

	clientConfig := &configs.YBClientConfig{
		MasterHostPort: strings.Split(cfg.masters, ","),
		OpTimeout:      time.Second * 10,
	}

	ybdbClient := client.NewYBClient(clientConfig)
	if err := ybdbClient.Connect(); err != nil {
		logger.Error("failed connecting to the cluster", "reason", err)
		return 1
	}
	defer ybdbClient.Close()

	logger.Info("connected to the cluster")

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
		logger.Info("created a new CDC stream")
	} else {
		reverseParsedStreamID, err := ybdbid.TryParseFromString(cfg.stream)
		if err != nil {
			logger.Error("failed parsing given CDC stream ID to bytes", "reason", err)
			return 1
		}
		streamIDBytes = reverseParsedStreamID.Bytes()
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

func consume(ctx context.Context,
	logger hclog.Logger,
	loggerClient hclog.Logger,
	cp *clientProvider,
	streamID, tabletID []byte) {

	checkpoint := &ybApi.CDCCheckpointPB{
		OpId: &ybApi.OpIdPB{
			Term:  pint64(0),
			Index: pint64(0),
		},
	}

	<-time.After(time.Millisecond * time.Duration(rand.Intn(100-10)+10))

	for {

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Millisecond * 500):
			// reiterate
		}

		c, err := cp.getClient(loggerClient)
		if err != nil {
			logger.Error("failed fetching a client", "reason", err)
			continue
		}

		request := &ybApi.GetChangesRequestPB{
			StreamId:       streamID,
			TabletId:       tabletID,
			FromCheckpoint: checkpoint,
		}

		response := &ybApi.GetChangesResponsePB{}
		requestErr := c.Execute(request, response)

		if requestErr != nil {
			logger.Error("failed fetching changes", "reason", requestErr)
			continue
		}

		if err := errors.NewCDCError(response.Error); err != nil {
			logger.Error("failed fetching changes", "reason", err)
			continue
		}

		if len(response.Records) == 0 {
			continue
		}

		if compareOpId(checkpoint.OpId, response.Checkpoint.OpId) == 1 {
			bs, err := json.MarshalIndent(response, "", "  ")
			if err != nil {
				logger.Error("failed marshaling JSON", "reason", err)
				continue
			}
			fmt.Println(string(bs))
			checkpoint = response.Checkpoint
		}

	}

}
