package log2clickhouse

import (
	"encoding/json"
	"io"
	"net"
	"time"

	"github.com/xaionaro-go/errors"
)

type InputRawJSON struct {
	Logger Logger
	Reader io.ReadCloser

	TableName string
	Columns   []string

	OutChan chan *Row

	isRunning bool
}

func NewInputRawJSON(
	reader io.ReadCloser,
	OutChan chan *Row,
	tableName string,
	dataColumnName string,
	dateColumnName string,
	nanosecondsColumnName string,
	logger Logger,
) *InputRawJSON {
	input := &InputRawJSON{}

	if logger == nil {
		logger = dummyLogger
	}
	input.Logger = logger

	input.OutChan = OutChan

	input.Reader = reader
	input.TableName = tableName
	input.Columns = []string{dateColumnName}
	if nanosecondsColumnName != "" {
		input.Columns = append(input.Columns, nanosecondsColumnName)
	}
	input.Columns = append(input.Columns, dataColumnName)

	input.start()

	return input
}

func (l *InputRawJSON) isNanoTSEnabled() bool {
	return len(l.Columns) == 3
}

func (l *InputRawJSON) start() {
	go l.loop()
}

func (l *InputRawJSON) loop() {
	decoder := json.NewDecoder(l.Reader)

	msg := json.RawMessage{}
	l.isRunning = true
	for l.isRunning {
		msg = msg[:0]

		l.Logger.Trace(`S`)
		err := decoder.Decode(&msg)
		l.Logger.Trace(`/S`)
		if err != nil {
			if !l.isRunning {
				break
			}
			if err == io.EOF {
				// Closed by other side
				l.Close()
				continue
			}
			if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
				// Timeout
				l.Close()
				continue
			}
			buf := newBuffer()
			buf.ReadFrom(decoder.Buffered()) // TODO: implement the method "Buffered()" within "gojay"
			l.Logger.Warning(errors.Wrap(err, `(*InputRawJSON).loop(): unable to decode`), buf.String())
			buf.Release()

			// TODO: remove this dirty hack. It's required to find a way to just reset the decoder
			// (instead of re-creating it)
			decoder = json.NewDecoder(l.Reader)
			continue
		}
		now := time.Now()

		row := NewRow()
		row.tableName = l.TableName
		row.columns = l.Columns
		if l.isNanoTSEnabled() {
			row.values = []interface{}{now, now.UnixNano(), string(msg)}
		} else {
			row.values = []interface{}{now, string(msg)}
		}
		l.Logger.Trace(`Q`)
		select {
		case l.OutChan <- row:
		default:
			l.Logger.Error("the queue is busy")
		}
		l.Logger.Trace(`/Q`)
	}
}

func (l *InputRawJSON) Close() error {
	l.isRunning = false
	return l.Reader.Close()
}
