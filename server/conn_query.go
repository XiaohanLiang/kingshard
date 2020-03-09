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

package server

import (
	"fmt"
	"runtime"
	"time"

	"github.com/XiaohanLiang/kingshard/lib/parser"

	"github.com/XiaohanLiang/kingshard/backend"
	"github.com/XiaohanLiang/kingshard/lib/errors"
	"github.com/XiaohanLiang/kingshard/lib/golog"
	"github.com/XiaohanLiang/kingshard/mysql"
)

func (c *ClientConn) handleQuery(sql string) (err error) {

	defer func() {
		if e := recover(); e != nil {
			golog.OutputSql("Error", "err:%v,sql:%s", e, sql)

			if err, ok := e.(error); ok {
				const size = 4096
				buf := make([]byte, size)
				buf = buf[:runtime.Stack(buf, false)]

				golog.Error("ClientConn", "handleQuery",
					err.Error(), 0,
					"stack", string(buf), "sql", sql)
			}

			err = errors.ErrInternalServer
			return
		}
	}()

	var (
		rs  []*mysql.Result
		log golog.Log
	)

	action, tables, dbs, err := parser.Parse(sql)
	if err != nil {
		return err
	}

	conn, err = c.getBackendConn(GetServer(), false)
	defer c.closeConn(conn, false)
	if err != nil {
		return
	}

	rs, log, err = c.executeInNode(conn, sql, action, tables, dbs)
	if err != nil {
		return err
	}

	if isSpecialAction(action) {
		return
	}

	if len(rs) != 0 && rs[0] != nil && rs[0].Resultset != nil {
		err = c.writeResultset(c.status, rs[0].Resultset)
	} else {
		err = c.writeOK(rs[0])
	}

	log.Type = "output"
	log.Sql = rs[0].GetLog(log.State)
	golog.Logging(log)

	return err
}

func (c *ClientConn) getBackendConn(n *backend.Node, fromSlave bool) (co *backend.BackendConn, err error) {
	if !c.isInTransaction() {

		if fromSlave {
			co, err = n.GetSlaveConn()
			if err != nil {
				co, err = n.GetMasterConn()
			}
		} else {
			co, err = n.GetMasterConn()
		}
		if err != nil {
			golog.Error("server", "getBackendConn", err.Error(), 0)
			return
		}
	} else {
		var ok bool
		co, ok = c.txConns[n]

		if !ok {
			if co, err = n.GetMasterConn(); err != nil {
				return
			}

			if !c.isAutoCommit() {
				if err = co.SetAutoCommit(0); err != nil {
					return
				}
			} else {
				if err = co.Begin(); err != nil {
					return
				}
			}

			c.txConns[n] = co
		}
	}

	if err = co.UseDB(c.db); err != nil {
		//reset the database to null
		c.db = ""
		return
	}

	if err = co.SetCharset(c.charset, c.collation); err != nil {
		return
	}

	return
}

func isSpecialAction(action string) bool {
	return action == "Begin" || action == "Commit" || action == "Rollback"
}

func (c *ClientConn) handleSpecialAction(action string) error {

	switch action {
	case "Begin":
		return c.handleBegin()
	case "Commit":
		return c.handleCommit()
	case "Rollback":
		return c.handleRollback()
	}

	return errors.ErrCmdUnsupport
}

func (c *ClientConn) executeInNode(conn *backend.BackendConn, sql string, action string, tables []string, dbs []string) ([]*mysql.Result, golog.Log, error) {

	var (
		state     string
		err       error
		r         *mysql.Result
		log       golog.Log
		startTime = time.Now().UnixNano()
	)

	if isSpecialAction(action) {
		c.txConns[GetServer()] = conn
		err = c.handleSpecialAction(action)
	} else {
		r, err = conn.Execute(sql)
	}

	if err != nil {
		state = "ERROR"
	} else {
		state = "OK"
	}

	log = golog.Log{
		Type:        "input",
		Operator:    c.user,
		OperateTime: time.Now().Unix(),
		Duration:    float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond),
		State:       state,
		Action:      action,
		Table:       fmt.Sprintf("%v", tables),
		Database:    fmt.Sprintf("%v", dbs),
		Sql:         sql,
		TargetIp:    Addr,
		SourceIp:    c.c.RemoteAddr().String(),
	}
	golog.Logging(log)

	return []*mysql.Result{r}, log, err
}

func (c *ClientConn) closeConn(conn *backend.BackendConn, rollback bool) {
	if c.isInTransaction() {
		return
	}

	if rollback {
		conn.Rollback()
	}

	conn.Close()
}
