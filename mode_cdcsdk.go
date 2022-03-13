package main

import (
	"context"
	"encoding/json"
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
	"github.com/radekg/yugabyte-db-go-client/errors"
	"github.com/radekg/yugabyte-db-go-client/utils/ybdbid"
	ybApi "github.com/radekg/yugabyte-db-go-proto/v2/yb/api"
)

func executeCDCSDK(ybdbClient client.YBClient,
	logger hclog.Logger,
	loggerClient hclog.Logger,
	cfg *cdcConfig) int {

	tables := []*ybApi.ListTablesResponsePB_TableInfo{}

	listTablesResponse, err := listTables(ybdbClient, cfg.database)
	if err != nil {
		logger.Error("failed listing database tables", "reason", err)
		return 1
	}
	if err := errors.NewMasterError(listTablesResponse.GetError()); err != nil {
		logger.Error("failed listing database tables", "reason", err)
		return 1
	}

	for _, tb := range listTablesResponse.Tables {
		if tb.RelationType == nil {
			continue
		}
		if tb.Namespace == nil {
			continue
		}
		if *tb.RelationType == ybApi.RelationType_INDEX_TABLE_RELATION || *tb.RelationType == ybApi.RelationType_SYSTEM_TABLE_RELATION {
			continue
		}
		if *tb.Namespace.Name != cfg.database {
			continue
		}

		tableID, err := ybdbid.TryParseFromBytes(tb.Id)
		if err != nil {
			logger.Error("failed parsing table ID as string", "reason", err)
			continue
		}

		fullName := strings.Join([]string{
			*tb.Namespace.Name,
			*tb.PgschemaName,
			*tb.Name,
		}, ".")

		fmt.Println(" =========>", fullName, "=>", tableID.String())

		tables = append(tables, tb)
	}

	// The CDC framework takes the first table from listed tables
	// and uses it to discover tablet servers.
	// https://github.com/yugabyte/debezium/blob/final-connector-ybdb/debezium-connector-yugabytedb2/src/main/java/io/debezium/connector/yugabytedb/YugabyteDBConnector.java#L293-L310
	// This probably isn't the most ergonomic way of doing it
	// because there could exist tables which aren't designated for
	// every TServer on the cluster but whatever...

	if len(tables) == 0 {
		logger.Error("no tables available")
		return 1
	}

	tabletLocations, err := listTabletLocations(ybdbClient, tables[0].Id)
	if err != nil {
		logger.Error("error while listing tablet locations", "reason", err)
		return 1
	}

	if len(tabletLocations) == 0 {
		logger.Error("no tablet locations to run the CDC against")
		return 1
	}

	logger.Info("found tablet locations", "num-tablet-locations", len(tabletLocations))

	hostPorts := []*ybApi.HostPortPB{}
	for _, location := range tabletLocations {
		for _, replica := range location.Replicas {
			hostPorts = append(hostPorts, replica.TsInfo.BroadcastAddresses...)
			hostPorts = append(hostPorts, replica.TsInfo.PrivateRpcAddresses...)
		}
	}
	hostPorts = getReachableHostPorts(hostPorts)

	cp := newClientProvider(hostPorts)

	var streamIDBytes []byte
	for {
		c, err := cp.getClient(loggerClient)
		if err != nil {
			logger.Error("could not get connected client, going to retry", "reason", err)
			<-time.After(time.Millisecond * 100)
			continue
		}
		streamResponse, err := createDatabaseCDCStream(c, cfg.database)
		if err != nil {
			c.Close()
			logger.Error("error creating new CDC SDK stream, going to retry", "reason", err)
			<-time.After(time.Millisecond * 100)
			continue
		}
		c.Close()
		streamIDBytes = streamResponse.DbStreamId
		break
	}

	parsedStreamID, err := ybdbid.TryParseFromBytes(streamIDBytes)
	if err != nil {
		logger.Error("failed parsing new stream ID", "reason", err)
		return 1
	}
	logger = logger.With("stream-id", parsedStreamID.String())
	logger.Info("created a new CDC SDK stream")

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

	allTableLocations := []*ybApi.TabletLocationsPB{}
	for _, table := range tables {

		tableLocations, err := listTabletLocations(ybdbClient, table.Id)
		if err != nil {
			logger.Error("failed listing table locations for table",
				"table", *table.Name,
				"reason", err)
			return 1
		}
		allTableLocations = append(allTableLocations, tableLocations...)
	}

	// start consuming:
	wg := &sync.WaitGroup{}
	for _, location := range allTableLocations {
		wg.Add(1)
		go func(tabletID []byte) {
			consumeCDCSDK(ctx, logger, loggerClient, cp, streamIDBytes, tabletID)
			wg.Done()
		}(location.TabletId)
	}
	wg.Wait()

	return 0
}

func consumeCDCSDK(ctx context.Context,
	logger hclog.Logger,
	loggerClient hclog.Logger,
	cp *clientProvider,
	streamID, tabletID []byte) {

	checkpoint := &ybApi.CDCSDKCheckpointPB{}

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
			StreamId:             streamID,
			TabletId:             tabletID,
			FromCdcSdkCheckpoint: checkpoint,
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

		if len(response.CdcSdkProtoRecords) == 0 {
			continue
		}

		bs, err := json.MarshalIndent(response, "", "  ")
		if err != nil {
			logger.Error("failed marshaling JSON", "reason", err)
			continue
		}
		fmt.Println(string(bs))
		checkpoint = response.CdcSdkCheckpoint

	}

}
