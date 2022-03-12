package main

import (
	"fmt"
	"strings"

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

		// The CDC framework takes the first table from listed tables
		// and uses it to discover tablet servers.
		// https://github.com/yugabyte/debezium/blob/final-connector-ybdb/debezium-connector-yugabytedb2/src/main/java/io/debezium/connector/yugabytedb/YugabyteDBConnector.java#L293-L310

		fmt.Println(" =========>", fullName, "=>", tableID.String())
	}

	return 0

	return 0
}
