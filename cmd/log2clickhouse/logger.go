package main

import (
	"log"

	"github.com/xaionaro-go/log2clickhouse"
)

var (
	isTracingEnabled = false
)

var (
	_ log2clickhouse.Logger = &logger{}
)

type logger struct {
	warnCounter int
}

func (l *logger) Error(args ...interface{}) {
	log.Print(`[error] `, args)
}

func (l *logger) Warning(args ...interface{}) {
	if l.warnCounter%1000 == 0 {
		log.Print(`[warning] `, args)
	}
	l.warnCounter++
}

func (l *logger) Info(args ...interface{}) {
	log.Print(`[info] `, args)
}

func (l *logger) Trace(args ...interface{}) {
	if !isTracingEnabled {
		return
	}
	log.Print(`[trace] `, args)
}
