package log2clickhouse

import (
	"github.com/xaionaro-go/errors"
)

var (
	ErrTooMuchRowsInQueue = errors.New(`too much rows in the queue (cannot write into ClickHouse?)`)
)
