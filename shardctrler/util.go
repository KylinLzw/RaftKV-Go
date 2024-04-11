package shardctrler

// 日志打印

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

type logTopic string

const (
	DError logTopic = "ERRO" // level = 3
	DWarn  logTopic = "WARN" // level = 2
	DInfo  logTopic = "INFO" // level = 1
	DDebug logTopic = "DBUG" // level = 0

	// level = 1
	DMachine logTopic = "ATOM"
)

func getTopicLevel(topic logTopic) int {
	switch topic {
	case DError:
		return 3
	case DWarn:
		return 2
	case DInfo:
		return 1
	case DDebug:
		return 0
	default:
		return 1
	}
}

func getEnvLevel() int {
	v := os.Getenv("VERBOSE")
	//v = "0"
	level := getTopicLevel(DError) + 1
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var logStart time.Time
var logLevel int

func init() {
	logLevel = getEnvLevel()
	logStart = time.Now()

	// do not print verbose date
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

var LEAVEL = 1

func MyLog(peerId int, topic logTopic, format string, a ...interface{}) {
	topicLevel := getTopicLevel(topic)
	logLevel = LEAVEL
	if logLevel <= topicLevel {
		time := time.Since(logStart).Microseconds()
		time /= 100
		var prefix string
		if peerId == -1 {
			prefix = fmt.Sprintf("\033[33m%06d ----- %v\033[0m ", time, string(topic))
		} else {
			prefix = fmt.Sprintf("\033[33m%06d ----- %v\033[0m S%d ", time, string(topic), peerId)
		}
		format = prefix + format
		log.Printf(format, a...)

		/*
			重置（无颜色）：\033[0m
			红色：\033[31m
			绿色：\033[32m
			黄色：\033[33m
			蓝色：\033[34m
			品红（洋红色）：\033[35m
			青色：\033[36m
			白色：\033[37m
		*/
	}
}
