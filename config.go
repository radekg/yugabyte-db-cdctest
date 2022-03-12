package main

const defaultLogLevel = "info"

type cdcConfig struct {
	mode           string
	database       string
	logAsJSON      bool
	logLevel       string
	logLevelClient string
	masters        string
	stream         string
	table          string
}
