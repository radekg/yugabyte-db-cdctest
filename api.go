package main

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/radekg/yugabyte-db-go-client/client"
	"github.com/radekg/yugabyte-db-go-client/configs"
	"github.com/radekg/yugabyte-db-go-client/errors"
	"github.com/radekg/yugabyte-db-go-client/utils/ybdbid"
	ybApi "github.com/radekg/yugabyte-db-go-proto/v2/yb/api"
)

func createDatabaseCDCStream(connectedSingleNodeClient client.YBConnectedClient, namespaceName string) (*ybApi.CreateCDCStreamResponsePB, error) {
	request := &ybApi.CreateCDCStreamRequestPB{
		//TableId: pstring(parsedTableID.String()),
		NamespaceName: pstring(namespaceName),
		RecordType: func() *ybApi.CDCRecordType {
			v := ybApi.CDCRecordType_CHANGE
			return &v
		}(),
		RecordFormat: func() *ybApi.CDCRecordFormat {
			v := ybApi.CDCRecordFormat_PROTO
			return &v
		}(),
		SourceType: func() *ybApi.CDCRequestSource {
			v := ybApi.CDCRequestSource_CDCSDK
			return &v
		}(),
		CheckpointType: func() *ybApi.CDCCheckpointType {
			v := ybApi.CDCCheckpointType_EXPLICIT
			return &v
		}(),
	}
	response := &ybApi.CreateCDCStreamResponsePB{}
	requestErr := connectedSingleNodeClient.Execute(request, response)
	if requestErr != nil {
		return nil, requestErr
	}
	return response, errors.NewCDCError(response.Error)
}

func createCDCStream(connectedSingleNodeClient client.YBConnectedClient, tableID []byte) (*ybApi.CreateCDCStreamResponsePB, error) {
	parsedTableID, err := ybdbid.TryParseFromBytes(tableID)
	if err != nil {
		return nil, err
	}
	request := &ybApi.CreateCDCStreamRequestPB{
		TableId: pstring(parsedTableID.String()),
		RecordType: func() *ybApi.CDCRecordType {
			v := ybApi.CDCRecordType_CHANGE
			return &v
		}(),
		RecordFormat: func() *ybApi.CDCRecordFormat {
			v := ybApi.CDCRecordFormat_JSON
			return &v
		}(),
		SourceType: func() *ybApi.CDCRequestSource {
			v := ybApi.CDCRequestSource_XCLUSTER
			return &v
		}(),
		CheckpointType: func() *ybApi.CDCCheckpointType {
			v := ybApi.CDCCheckpointType_IMPLICIT
			return &v
		}(),
	}
	response := &ybApi.CreateCDCStreamResponsePB{}
	requestErr := connectedSingleNodeClient.Execute(request, response)
	if requestErr != nil {
		return nil, requestErr
	}
	return response, errors.NewCDCError(response.Error)
}

func getLastOpIdRequestPB(connectedSingleNodeClient client.YBConnectedClient, tabletID []byte) (*ybApi.OpIdPB, error) {
	// skip previous entries:
	getLastOpIdRequest := &ybApi.GetLatestEntryOpIdRequestPB{
		TabletId: tabletID,
	}
	getLastOpIdResponse := &ybApi.GetLatestEntryOpIdResponsePB{}
	if err := connectedSingleNodeClient.Execute(getLastOpIdRequest, getLastOpIdResponse); err != nil {
		return nil, err
	}
	if err := errors.NewCDCError(getLastOpIdResponse.Error); err != nil {
		return nil, err
	}
	return getLastOpIdResponse.OpId, nil
}

func getReachableHostPorts(hostPorts []*ybApi.HostPortPB) []*ybApi.HostPortPB {
	// Given a list of host ports,
	// discover and keep only those host ports to which we can connect.
	// This adds overhead at the start, but shortens
	// subsequent connects if the cluster is configured
	// with host ports which cannot be reached from the cdc client.
	newHostPorts := []*ybApi.HostPortPB{}
	for _, hp := range hostPorts {
		singleNodeConfig := &configs.YBSingleNodeClientConfig{
			MasterHostPort: fmt.Sprintf("%s:%d", *hp.Host, *hp.Port),
		}
		singleNodeClient, err := client.NewDefaultConnector().Connect(singleNodeConfig)
		if err == nil {
			singleNodeClient.Close()
			newHostPorts = append(newHostPorts, hp)
		}
	}
	return newHostPorts
}

func getSingleNodeClient(hostPorts []*ybApi.HostPortPB, logger hclog.Logger) (client.YBConnectedClient, error) {
	chanClient := make(chan client.YBConnectedClient)
	go func() {
	outer:
		for {
			r := rand.Intn(len(hostPorts))
			singleNodeConfig := &configs.YBSingleNodeClientConfig{
				MasterHostPort: fmt.Sprintf("%s:%d", *hostPorts[r].Host, *hostPorts[r].Port),
			}
			singleNodeClient, err := client.NewDefaultConnector().
				WithLogger(logger).
				Connect(singleNodeConfig)
			if err != nil {
				<-time.After(time.Millisecond * 100)
				continue
			}
			select {
			case <-singleNodeClient.OnConnected():
				chanClient <- singleNodeClient
				break outer
			case <-singleNodeClient.OnConnectError():
				<-time.After(time.Millisecond * 100)
				continue
			}
		}
	}()
	select {
	case c := <-chanClient:
		return c, nil
	case <-time.After(time.Second * 10):
		return nil, fmt.Errorf("timed out")
	}
}

func listHostPorts(ybdbClient client.YBClient) ([]*ybApi.HostPortPB, error) {

	request := &ybApi.ListTabletServersRequestPB{}
	response := &ybApi.ListTabletServersResponsePB{}
	requestErr := ybdbClient.Execute(request, response)

	hostPorts := []*ybApi.HostPortPB{}

	if requestErr != nil {
		return nil, requestErr
	}

	if err := errors.NewMasterError(response.Error); err != nil {
		return nil, err
	}

	for _, ts := range response.Servers {
		hostPorts = append(hostPorts, ts.Registration.Common.PrivateRpcAddresses...)
	}
	for _, ts := range response.Servers {
		hostPorts = append(hostPorts, ts.Registration.Common.BroadcastAddresses...)
	}

	return getReachableHostPorts(hostPorts), nil
}

func listTables(ybdbClient client.YBClient, database string) (*ybApi.ListTablesResponsePB, error) {
	request := &ybApi.ListTablesRequestPB{
		Namespace: &ybApi.NamespaceIdentifierPB{
			// ask for tables of the requested database:
			Name: pstring(database),
			// ask for PGSQL tables only:
			DatabaseType: pYQLDatabase(ybApi.YQLDatabase_YQL_DATABASE_PGSQL),
		},
		RelationTypeFilter: []ybApi.RelationType{
			// ask for user tables only:
			ybApi.RelationType_USER_TABLE_RELATION,
		},
	}

	response := &ybApi.ListTablesResponsePB{}
	requestErr := ybdbClient.Execute(request, response)

	if requestErr != nil {
		return nil, requestErr
	}

	return response, errors.NewMasterError(response.Error)
}

func listEligibleTables(ybdbClient client.YBClient, database string) ([]*ybApi.ListTablesResponsePB_TableInfo, error) {
	tables := []*ybApi.ListTablesResponsePB_TableInfo{}
	listTablesResponse, err := listTables(ybdbClient, database)
	if err != nil {
		return tables, err
	}
	if err := errors.NewMasterError(listTablesResponse.GetError()); err != nil {
		return tables, err
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
		if *tb.Namespace.Name != database {
			continue
		}
		tables = append(tables, tb)
	}
	return tables, nil
}

func listTabletLocations(ybdbClient client.YBClient, tableID []byte) ([]*ybApi.TabletLocationsPB, error) {
	request := &ybApi.GetTableLocationsRequestPB{
		Table: &ybApi.TableIdentifierPB{
			TableId: tableID,
		},
	}
	response := &ybApi.GetTableLocationsResponsePB{}
	requestErr := ybdbClient.Execute(request, response)
	if requestErr != nil {
		return nil, requestErr
	}
	return response.TabletLocations, errors.NewMasterError(response.Error)
}

func compareOpId(old, new *ybApi.OpIdPB) int {
	if *old.Index == *new.Index && *old.Term == *new.Term {
		return 0 // the same
	}
	if *old.Index < *new.Index || *old.Term < *new.Term {
		return 1 // new is bigger than old
	}
	return -1 // old is bigger than new
}

func waitForTableCreateDone(ybdbClient client.YBClient, tableID []byte) error {
	chanDone := make(chan struct{})
	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()
	go func() {
		for {

			select {
			case <-ctx.Done():
				return
			default:
			}

			request := &ybApi.IsCreateTableDoneRequestPB{
				Table: &ybApi.TableIdentifierPB{
					TableId: tableID,
				},
			}
			response := &ybApi.IsCreateTableDoneResponsePB{}
			requestErr := ybdbClient.Execute(request, response)
			if requestErr != nil {
				<-time.After(time.Millisecond * 100)
				continue
			}
			if err := errors.NewMasterError(response.Error); err != nil {
				<-time.After(time.Millisecond * 100)
				continue
			}
			if *response.Done {
				close(chanDone)
				return // done
			}

		}
	}()
	select {
	case <-chanDone:
		cancelFunc() // cancel the context,
		return nil
	case <-time.After(time.Second * 30): // configurable timeout?
		return fmt.Errorf("timed out")
	}
}
