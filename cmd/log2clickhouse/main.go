package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/xaionaro-go/log2clickhouse"
)

func fatalIf(err error) {
	if err == nil {
		return
	}

	log.Fatal(err)
}

func newInput(
	reader io.ReadCloser,
	rowsChannel chan *log2clickhouse.Row,
	inputFormat string,
	tableName string,
	dataColumnName string,
	dateColumnName string,
	nanosecondsColumnName string,
	tryParseJSONInFields string,
	logger log2clickhouse.Logger,
) io.Closer {
	switch inputFormat {
	case `json`:
		return log2clickhouse.NewInputJSON(
			reader,
			rowsChannel,
			tableName,
			dateColumnName,
			nanosecondsColumnName,
			strings.Split(tryParseJSONInFields, ","),
			logger,
		)
	case `rawjson`:
		return log2clickhouse.NewInputRawJSON(
			reader,
			rowsChannel,
			tableName,
			dataColumnName,
			dateColumnName,
			nanosecondsColumnName,
			logger,
		)
	default:
		log.Fatalf(`unknown input format: "%v"`, inputFormat)
	}
	return nil
}

func main() {
	var udpPort = flag.Int(`udp-port`, 5363, `UDP port to be listened (to disable: -1; default: 5363)`)
	var tcpPort = flag.Int(`tcp-port`, 5363, `TCP port to be listened (to disable: -1; default: 5363)`)
	var netPprofPort = flag.Int(`net-pprof-port`, 5364, `port to be used for "net/pprof" (to disable: -1; default: 5364)`)
	var tableName = flag.String(`table-name`, `log`, `table to be inserted to (default: "log")`)
	var inputFormat = flag.String(`input-format`, `json`, `input data format (possible values: json, rawjson; default: "json")`)
	var tryParseJSONInFields = flag.String(`try-parse-json-in-fields`, `message`, `only for input data format "json"; comma separated values (default value: "message"; to disable: "")`)
	var dataColumnName = flag.String(`data-column-name`, `rawjson`, `if input format is "rawjson" then it's required to select a column to write to (default: "rawjson")`)
	var dateColumnName = flag.String(`date-column-name`, `date`, `if input format is "rawjson" then it's required to select a column to be used for log receive dates (default: "date")`)
	var nanosecondsColumnName = flag.String("nanoseconds-column-name", `ts`, `column name to store the timestamp with nanoseconds (if empty then do not store it)`)
	var chAddr = flag.String(`ch-addr`, `127.0.0.1:9000`, `address of the ClickHouse server`)
	var chDBName = flag.String(`ch-db`, `log`, `DB name`)
	var chUser = flag.String(`ch-user`, `default`, `username to auth in ClickHouse`)
	var chPassword = flag.String("ch-pass", ``, `password to auth in ClickHouse`)
	var tracingEnabled = flag.Bool("log-verbose", false, `enable verbose logging`)
	flag.Parse()
	isTracingEnabled = *tracingEnabled

	rowsChannel := make(chan *log2clickhouse.Row, 65536)

	if *udpPort >= 0 {
		conn, err := net.ListenUDP("udp", &net.UDPAddr{Port: *udpPort})
		fatalIf(err)

		newInput(
			conn,
			rowsChannel,
			*inputFormat,
			*tableName,
			*dataColumnName,
			*dateColumnName,
			*nanosecondsColumnName,
			*tryParseJSONInFields,
			&logger{},
		)
	}

	if *tcpPort >= 0 {
		listener, err := net.ListenTCP("tcp", &net.TCPAddr{Port: *tcpPort})
		fatalIf(err)

		go func() {
			for {

				conn, err := listener.Accept()
				fatalIf(err)
				newInput(
					conn,
					rowsChannel,
					*inputFormat,
					*tableName,
					*dataColumnName,
					*dateColumnName,
					*nanosecondsColumnName,
					*tryParseJSONInFields,
					&logger{},
				)
			}
		}()
	}

	if *netPprofPort >= 0 {
		go func() {
			fatalIf(http.ListenAndServe(fmt.Sprintf(`:%v`, *netPprofPort), nil))
		}()
	}

	chInserter, err := log2clickhouse.NewCHInserter(*chAddr, *chDBName, *chUser, *chPassword, rowsChannel, &logger{})
	fatalIf(err)
	fatalIf(chInserter.Loop(time.Second))
}
