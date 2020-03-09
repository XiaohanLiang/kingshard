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
	"github.com/XiaohanLiang/kingshard/backend"
)

type ExecuteDB struct {
	ExecNode *backend.Node
	IsSlave  bool
	sql      string
}

func (c *ClientConn) GetTransExecDB(sql string) (*ExecuteDB, error) {
	var err error
	executeDB := new(ExecuteDB)
	executeDB.sql = sql

	executeDB.IsSlave = false
	executeDB, err = c.GetExecDB(sql)
	if err != nil {
		return nil, err
	}
	if executeDB == nil {
		return nil, nil
	}
	return executeDB, nil
}

func (c *ClientConn) GetExecDB(sql string) (*ExecuteDB, error) {
	return c.getSelectExecDB(sql)
}

func (c *ClientConn) getSelectExecDB(sql string) (*ExecuteDB, error) {
	executeDB := new(ExecuteDB)
	executeDB.sql = sql
	executeDB.IsSlave = true
	return executeDB, nil
}
