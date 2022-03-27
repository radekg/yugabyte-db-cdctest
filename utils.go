package main

import ybApi "github.com/radekg/yugabyte-db-go-proto/v2/yb/api"

func pint32(input int32) *int32 {
	return &input
}

func pint64(input int64) *int64 {
	return &input
}

func puint64(input uint64) *uint64 {
	return &input
}

func pstring(input string) *string {
	return &input
}

func pYQLDatabase(input ybApi.YQLDatabase) *ybApi.YQLDatabase {
	return &input
}
