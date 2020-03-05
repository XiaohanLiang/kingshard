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
	"strings"
	"time"

	"github.com/XiaohanLiang/kingshard/lib/parser"

	"github.com/XiaohanLiang/kingshard/backend"
	"github.com/XiaohanLiang/kingshard/lib/errors"
	"github.com/XiaohanLiang/kingshard/lib/golog"
	"github.com/XiaohanLiang/kingshard/lib/hack"
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
		rs        []*mysql.Result
		executeDB *ExecuteDB
	)

	action, _, _, err := parser.Parse(sql)
	if err != nil {
		return err
	}

	tokens := strings.FieldsFunc(sql, hack.IsSqlSep)
	if len(tokens) == 0 {
		return errors.ErrCmdUnsupport
	}

	// Key-2
	if c.isInTransaction() {
		executeDB, err = c.GetTransExecDB(tokens, sql)
	} else {
		executeDB, err = c.GetExecDB(tokens, sql)
	}

	if err != nil {
		//this SQL doesn't need execute in the backend.
		if err == errors.ErrIgnoreSQL {
			err = c.writeOK(nil)
			if err != nil {
				return err
			}
			return nil
		}
		return err
	}

	if executeDB == nil {
		return errors.ErrNoDatabase
	}

	conn, err = c.getBackendConn(GetServer(), false)
	if err != nil {
		return
	}

	defer c.closeConn(conn, false)
	if err != nil {
		return err
	}

	// TODO: Select certain db instance before transaction!
	if action == "Begin" {
		c.handleBegin()
		hack.Yell("Begin transaction")
		return
	}
	if action == "Commit" {
		c.handleCommit()
		hack.Yell("Endof transaction")
		return
	}

	//execute.sql may be rewritten in getShowExecDB
	rs, err = c.executeInNode(conn, executeDB.sql, nil)
	if err != nil {
		return err
	}

	if len(rs) == 0 {
		msg := fmt.Sprintf("result is empty")
		golog.Error("ClientConn", "handleUnsupport", msg, 0, "sql", sql)
		return mysql.NewError(mysql.ER_UNKNOWN_ERROR, msg)
	}

	if rs[0].Resultset != nil {
		err = c.writeResultset(c.status, rs[0].Resultset)
	} else {
		err = c.writeOK(rs[0])
	}

	if err != nil {
		return err
	}

	return nil
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

func (c *ClientConn) executeInNode(conn *backend.BackendConn, sql string, args []interface{}) ([]*mysql.Result, error) {
	var state string
	startTime := time.Now().UnixNano()
	r, err := conn.Execute(sql, args...)
	if err != nil {
		state = "ERROR"
	} else {
		state = "OK"
	}
	execTime := float64(time.Now().UnixNano()-startTime) / float64(time.Millisecond)
	golog.Logging(golog.Log{
		OperateTime: time.Now().Unix(),
		Duration:    execTime,
		State:       state,
		Action:      "Select",
		Table:       "Users",
		Database:    "Test",
		Sql:         sql,
		TargetIp:    conn.GetAddr(),
		SourceIp:    c.c.RemoteAddr().String(),
	})

	if err != nil {
		return nil, err
	}

	return []*mysql.Result{r}, err
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
