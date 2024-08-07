package log2clickhouse

type Logger interface {
	Error(...interface{})
	Warning(...interface{})
	Info(...interface{})
	Trace(...interface{})
}

type dummyLoggerT struct {
}

var dummyLogger Logger = &dummyLoggerT{}

func (l *dummyLoggerT) Error(...interface{})   {}
func (l *dummyLoggerT) Warning(...interface{}) {}
func (l *dummyLoggerT) Info(...interface{})    {}
func (l *dummyLoggerT) Trace(...interface{})   {}
