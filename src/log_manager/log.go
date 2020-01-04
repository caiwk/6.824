package log

import (
	"fmt"
	"log"
	"os"
)

var logger =log.New(os.Stdout ,"", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)

type Level int

const (
	infoLevel Level = iota
	warnLevel
	errorLevel
	debugLevel
	fatalLevel
)

var levelNames = [...]string{
	debugLevel: "[DEBUG] ",
	infoLevel:  "[-INFO-] ",
	warnLevel:  "[~WARN~] ",
	errorLevel: "[*ERROR*] ",
	fatalLevel: "[!FATAL!] ",
}

func init(){
	//syscall.Umask(0)
	//file, err := os.OpenFile("csfs.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	//if err != nil {
	//	log.Println(err)
	//}
	//logger = log.New(file ,"", log.LstdFlags|log.Lshortfile|log.Lmicroseconds)
	//l := &lumberjack.Logger{
	//	Filename:   "csfs.log",
	//	MaxSize:    1000, // megabytes
	//	MaxBackups: 5,
	//	MaxAge:     28, //days
	//}
	//logger.SetOutput(l)
}

// String implementation.
func (l Level) String() string {
	return levelNames[l]
}

func Error(v ...interface{}) {
	logger.SetPrefix(errorLevel.String())
	logger.Output(2, fmt.Sprintln(v...))
}
func Info(v ...interface{}) {
	logger.SetPrefix(infoLevel.String())
	logger.Output(2, fmt.Sprintln(v...))
}
func Warn(v ...interface{}) {
	logger.SetPrefix(warnLevel.String())
	logger.Output(2, fmt.Sprintln(v...))
}
func Debug(v ...interface{}) {
	logger.SetPrefix(debugLevel.String())
	logger.Output(2, fmt.Sprintln(v...))
}
func Fatal(v ...interface{}) {
	logger.SetPrefix(fatalLevel.String())
	logger.Output(2, fmt.Sprintln(v...))
	os.Exit(1)
}
func Errorf(format string, v ...interface{}) {
	logger.SetPrefix(errorLevel.String())
	logger.Output(2, fmt.Sprintf(format, v...))
}
func Infof(format string, v ...interface{}) {
	logger.SetPrefix(infoLevel.String())
	logger.Output(2, fmt.Sprintf(format, v...))
}
func Warnf(format string, v ...interface{}) {
	logger.SetPrefix(warnLevel.String())
	logger.Output(2, fmt.Sprintf(format, v...))
}
func Debugf(format string, v ...interface{}) {
	logger.SetPrefix(debugLevel.String())
	logger.Output(2, fmt.Sprintf(format, v...))
}
func Fatalf(format string, v ...interface{}) {
	logger.SetPrefix(fatalLevel.String())
	logger.Output(2, fmt.Sprintf(format, v...))
}