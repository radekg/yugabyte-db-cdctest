package main

import ybApi "github.com/radekg/yugabyte-db-go-proto/v2/yb/api"

func pint64(input int64) *int64 {
	return &input
}

func pstring(input string) *string {
	return &input
}

func pYQLDatabase(input ybApi.YQLDatabase) *ybApi.YQLDatabase {
	return &input
}
