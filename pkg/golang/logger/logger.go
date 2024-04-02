package logger

import (
	"log"
	"os"
)

type Logger struct {
	*log.Logger
}

func NewLogger(path string) *Logger {
	f, err := openLogFile(path)
	if err != nil {
		panic(err)
	}
	return &Logger{log.New(f, "", log.LstdFlags|log.Lshortfile|log.Ltime)}
}

func openLogFile(path string) (*os.File, error) {
	logFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return logFile, nil
}
