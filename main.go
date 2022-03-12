package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/radekg/yugabyte-db-go-client/client"
	"github.com/radekg/yugabyte-db-go-client/configs"
	"github.com/radekg/yugabyte-db-go-client/errors"
	ybApi "github.com/radekg/yugabyte-db-go-proto/v2/yb/api"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {

	cfg := &cdcConfig{}

	flag.StringVar(&cfg.mode, "mode", "cdc", "mode to use: cdc or cdcsdk")
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

	if cfg.stream != "" {
		logger = logger.With("stream-id", cfg.stream)
	}

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

	if cfg.mode == "cdc" {
		return executeCDC(ybdbClient, logger, loggerClient, cfg)
	}

	if cfg.mode == "cdcsdk" {
		return executeCDCSDK(ybdbClient, logger, loggerClient, cfg)
	}

	logger.Error("Unsupported mode", "mode", cfg.mode)
	return 1

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
