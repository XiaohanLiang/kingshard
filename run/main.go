// Copyright 2016 The kingshard Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package run

import (
	"fmt"
	"github.com/XiaohanLiang/kingshard/lib/golog"
	"github.com/XiaohanLiang/kingshard/server"
	"path"
	"runtime"
	"strings"
)

const (
	sqlLogName = "sql.log"
	sysLogName = "sys.log"
	MaxLogSize = 1024 * 1024 * 1024
)


func Run() {

	var (
		logpath = ""
		addr    = "0.0.0.0:9696"
		//charset = ""
	)

	runtime.GOMAXPROCS(runtime.NumCPU())

	//when the log file size greater than 1GB, kingshard will generate a new file
	sysFilePath := path.Join(logpath, sysLogName)
	sysFile, err := golog.NewRotatingFileHandler(sysFilePath, MaxLogSize, 1)
	if err != nil {
		fmt.Printf("new log file error:%v\n", err.Error())
		return
	}
	sqlFilePath := path.Join(logpath, sqlLogName)
	sqlFile, err := golog.NewRotatingFileHandler(sqlFilePath, MaxLogSize, 1)
	if err != nil {
		fmt.Printf("new log file error:%v\n", err.Error())
		return
	}
	golog.GlobalSysLogger = golog.New(sysFile, golog.Lfile|golog.Ltime|golog.Llevel)
	golog.GlobalSqlLogger = golog.New(sqlFile, golog.Lfile|golog.Ltime|golog.Llevel)

	setLogLevel("debug")

	var svr *server.Server
	svr, err = server.NewServer(addr)
	if err != nil {
		golog.Error("main", "main", err.Error(), 0)
		return
	}

	defer func() {
		golog.GlobalSysLogger.Close()
		golog.GlobalSqlLogger.Close()
		svr.Close()
	}()
	
	svr.Run()
}

func setLogLevel(level string) {
	switch strings.ToLower(level) {
	case "debug":
		golog.GlobalSysLogger.SetLevel(golog.LevelDebug)
	case "info":
		golog.GlobalSysLogger.SetLevel(golog.LevelInfo)
	case "warn":
		golog.GlobalSysLogger.SetLevel(golog.LevelWarn)
	case "error":
		golog.GlobalSysLogger.SetLevel(golog.LevelError)
	default:
		golog.GlobalSysLogger.SetLevel(golog.LevelError)
	}
}
