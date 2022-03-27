package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
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

	var streamIDBytes []byte
	tableIDs := [][]byte{}

	if cfg.stream != "" {

		// If stream ID was provided, check that the stream exists.
		// If it exists, load all eligible tables configured on the stream.

		// validate that the stream exists, read tables based on CDC settings:
		reverseParsedStreamID, err := ybdbid.TryParseFromString(cfg.stream)
		if err != nil {
			logger.Error("failed parsing given CDC stream ID to bytes", "reason", err)
			return 1
		}

		streamIDBytes = reverseParsedStreamID.Bytes()
		streamInfo, getStreamErr := getCDCSDKStreamByID(ybdbClient, streamIDBytes)
		if getStreamErr != nil {
			logger.Error("failed fetching stream info for given stream ID",
				"reason", getStreamErr)
			return 1
		}

		for _, tableInfo := range streamInfo.TableInfo {
			tableIDs = append(tableIDs, tableInfo.TableId)
		}

		logger = logger.With("stream-id", cfg.stream)

		logger.Info("found existing stream", "stream-id", cfg.stream, "num-tables", len(tableIDs))

	} else {

		// If stream ID was not provided, find all tables for the database.
		tables, err := listEligibleTables(ybdbClient, cfg.database)
		if err != nil {
			logger.Error("failed listing database tables", "reason", err)
			return 1
		}
		for _, table := range tables {
			tableIDs = append(tableIDs, table.Id)
		}

	}

	if len(tableIDs) == 0 {
		logger.Error("no tables available")
		return 1
	}

	// The CDC framework takes the first table from listed tables
	// and uses it to discover tablet servers.
	// https://github.com/yugabyte/debezium/blob/final-connector-ybdb/debezium-connector-yugabytedb2/src/main/java/io/debezium/connector/yugabytedb/YugabyteDBConnector.java#L293-L310
	// This probably isn't the most ergonomic way of doing it
	// because there could exist tables which aren't designated for
	// every TServer on the cluster but whatever...

	tabletLocations, err := listTabletLocations(ybdbClient, tableIDs[0])
	if err != nil {
		logger.Error("error while listing tablet locations", "reason", err)
		return 1
	}

	if len(tabletLocations) == 0 {
		logger.Error("no tablet locations for first table ID")
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

	if cfg.stream == "" {

		// If stream ID wasn't provided, create a new stream.
		// Repeat until successfully created.
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

		logger.Info("created new CDC SDK stream", "stream-id", parsedStreamID.String(), "num-tables", len(tableIDs))

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

	allTableLocations := []*ybApi.TabletLocationsPB{}
	for _, tableID := range tableIDs {

		tableLocations, err := listTabletLocations(ybdbClient, tableID)
		if err != nil {
			logger.Error("failed listing table locations for table",
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
			consumeCDCSDK(ctx, logger, loggerClient, cp, streamIDBytes, tabletID, cfg.fromLatest)
			wg.Done()
		}(location.TabletId)
	}
	wg.Wait()

	return 0
}

func getCDCSDKStreamByID(ybdbClient client.YBClient, streamID []byte) (*ybApi.MasterGetCDCDBStreamInfoResponsePB, error) {
	// this request must go via master CDC SDK proxy:
	request := &ybApi.MasterGetCDCDBStreamInfoRequestPB{
		DbStreamId: streamID,
	}
	response := &ybApi.MasterGetCDCDBStreamInfoResponsePB{}
	requestErr := ybdbClient.Execute(request, response)
	if requestErr != nil {
		return nil, requestErr
	}
	if err := errors.NewMasterError(response.Error); err != nil {
		return nil, err
	}
	return response, nil
}

// Reference: https://github.com/yugabyte/debezium/blob/final-connector-ybdb/debezium-connector-yugabytedb2/src/main/java/io/debezium/connector/yugabytedb/YugabyteDBStreamingChangeEventSource.java#L243
func consumeCDCSDK(ctx context.Context,
	logger hclog.Logger,
	loggerClient hclog.Logger,
	cp *clientProvider,
	streamID, tabletID []byte,
	newest bool) {

	checkpoint := &ybApi.CDCSDKCheckpointPB{}

	<-time.After(time.Millisecond * time.Duration(rand.Intn(100-10)+10))

	c, err := cp.getClient(loggerClient)
	if err != nil {
		logger.Error("failed fetching a client", "reason", err)
		return
	}

	if newest {
		opid, err := getLastOpIdRequestPB(c, tabletID)
		if err != nil {
			logger.Error("failed fetching last opid", "reason", err)
			return
		}
		checkpoint.Term = opid.Term
		checkpoint.Index = opid.Index
	}

	for {

		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Millisecond * 500):
			// reiterate
		}

		request := &ybApi.GetChangesRequestPB{
			StreamId:             streamID,
			TabletId:             tabletID,
			FromCdcSdkCheckpoint: checkpoint,
		}

		response := &ybApi.GetChangesResponsePB{}
		requestErr := c.Execute(request, response)

		if requestErr != nil {

			if _, ok := requestErr.(*errors.ReceiveError); !ok {
				// if EOF error, don't log, nothing to read...
				logger.Error("failed fetching changes", "reason", requestErr)
			}

			if _, ok := requestErr.(*errors.RequiresReconnectError); ok {
				reconnectedClient, err := cp.getClient(loggerClient)
				if err != nil {
					logger.Error("failed reconnecting a client", "reason", err)
					return
				}
				c = reconnectedClient
			}

			continue
		}

		if err := errors.NewCDCError(response.Error); err != nil {
			logger.Error("failed fetching changes", "reason", err)
			continue
		}

		if len(response.CdcSdkProtoRecords) == 0 {
			continue
		}

		/*
			for _, record := range response.CdcSdkProtoRecords {
				// Use record.RowMessage.Op to find out what's the operation type.
				// The Op points to:
				// type RowMessage_Op int32
				//
				// const (
				// 	RowMessage_UNKNOWN  RowMessage_Op = -1
				// 	RowMessage_INSERT   RowMessage_Op = 0
				// 	RowMessage_UPDATE   RowMessage_Op = 1
				// 	RowMessage_DELETE   RowMessage_Op = 2
				// 	RowMessage_BEGIN    RowMessage_Op = 3
				// 	RowMessage_COMMIT   RowMessage_Op = 4
				// 	RowMessage_DDL      RowMessage_Op = 5
				// 	RowMessage_TRUNCATE RowMessage_Op = 6
				// 	RowMessage_READ     RowMessage_Op = 7
				// )
			}
		*/

		bs, err := json.MarshalIndent(response, "", "  ")
		if err != nil {
			logger.Error("failed marshaling JSON", "reason", err)
			continue
		}

		fmt.Println(string(bs))

		/*

			// Commit the checkpoint.
			// No idea if this supposed to work or what it supposed to do.
			// However, when I do this, TServers start doing leader elections on tablets.
			// I don't think this is what should be used for _checkpoint persistence_?

			setCheckpointRequest := &ybApi.SetCDCCheckpointRequestPB{
				// hmm, why do I have to set this?
				Checkpoint: &ybApi.CDCCheckpointPB{
					OpId: &ybApi.OpIdPB{
						Term:  response.CdcSdkCheckpoint.Term,
						Index: response.CdcSdkCheckpoint.Index,
					},
				},
				CdcSdkCheckpoint: response.CdcSdkCheckpoint,
				TabletId:         tabletID,
				StreamId:         streamID,
			}
			setCheckpointResponse := &ybApi.SetCDCCheckpointResponsePB{}

			setCheckpointErr := c.Execute(setCheckpointRequest, setCheckpointResponse)
			if setCheckpointErr != nil {
				logger.Error("failed setting checkpoint", "reason", setCheckpointErr)
				c.Close()
				continue
			}
			if err := errors.NewCDCError(setCheckpointResponse.Error); err != nil {
				logger.Error("failed setting checkpoint", "reason", err)
				c.Close()
				continue
			}

			logger.Info("checkpoint updated", "response", setCheckpointResponse)
		*/

		checkpoint = response.CdcSdkCheckpoint

	}

}
