package main

import (
	"flag"
	"io"

	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

//	"github.com/evoila/influxdb-firehose-nozzle/influxdbfirehosenozzle"
//	"github.com/evoila/influxdb-firehose-nozzle/nozzleconfig"
//        "github.com/evoila/influxdb-firehose-nozzle/logger"
//	"github.com/evoila/influxdb-firehose-nozzle/uaatokenfetcher"

        "github.com/evoila/influxdb-firehose-nozzle/influxdbfirehosenozzle"
        "github.com/evoila/influxdb-firehose-nozzle/nozzleconfig"
        "github.com/evoila/influxdb-firehose-nozzle/logger"
        "github.com/evoila/influxdb-firehose-nozzle/uaatokenfetcher"
)

var (
 	logFilePath = flag.String("logFile", "", "The agent log file, defaults to STDOUT")
 	logLevel    = flag.Bool("debug", false, "Debug logging")
     	configFile  = flag.String("config", "config/influxdb-firehose-nozzle.json", "Location of the nozzle config json file")
)
func main() {
	flag.Parse()
        
	log := logger.NewLogger(*logLevel, *logFilePath, "influxdb-firehose-nozzle", "")

	config, err := nozzleconfig.Parse(*configFile)
	if err != nil {
		log.Fatalf("Error parsing config: %s", err.Error())
	}

        tokenFetcher := uaatokenfetcher.New(
 		config.UAAURL,
 		config.Client,
 		config.ClientSecret,
 		config.SsLSkipVerify,
 		log,
 	)

	threadDumpChan := registerGoRoutineDumpSignalChannel()
	defer close(threadDumpChan)
	go dumpGoRoutine(threadDumpChan)

	go runServer()

	influxDbNozzle := influxdbfirehosenozzle.NewInfluxDbFirehoseNozzle(config, tokenFetcher, log)
	influxDbNozzle.Start()
}

func defaultResponse(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "{ \"status\" : \"running\" }")
}

func runServer() {
	port := os.Getenv("PORT")

	log := logger.NewLogger(*logLevel, *logFilePath, "influxdb-firehose-nozzle", "")
	log.Infof("Go Port from environment: " + port)

	if port == "" {
		port = "8000"
	}

	log.Infof("Starting server with port: " + port)

	http.HandleFunc("/", defaultResponse)
	http.ListenAndServe(":"+port, nil)
}

func registerGoRoutineDumpSignalChannel() chan os.Signal {
	threadDumpChan := make(chan os.Signal, 1)
	signal.Notify(threadDumpChan, syscall.SIGUSR1)

	return threadDumpChan
}

func dumpGoRoutine(dumpChan chan os.Signal) {
	for range dumpChan {
		goRoutineProfiles := pprof.Lookup("goroutine")
		if goRoutineProfiles != nil {
			goRoutineProfiles.WriteTo(os.Stdout, 2)
		}
	}
}
