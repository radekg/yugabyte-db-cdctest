package main

import (
	"flag"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/radekg/yugabyte-db-go-client/client"
	"github.com/radekg/yugabyte-db-go-client/configs"
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
	flag.BoolVar(&cfg.newest, "newest", false, "When set, skips all previous entries and continues from current operation")
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
