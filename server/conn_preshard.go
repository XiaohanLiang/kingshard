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
	"strings"

	"github.com/XiaohanLiang/kingshard/backend"
	"github.com/XiaohanLiang/kingshard/lib/errors"
	"github.com/XiaohanLiang/kingshard/mysql"
)

type ExecuteDB struct {
	ExecNode *backend.Node
	IsSlave  bool
	sql      string
}

func (c *ClientConn) GetTransExecDB(tokens []string, sql string) (*ExecuteDB, error) {
	var err error
	tokensLen := len(tokens)
	executeDB := new(ExecuteDB)
	executeDB.sql = sql

	//transaction execute in master db
	executeDB.IsSlave = false

	if 2 <= tokensLen {
		if tokens[0][0] == mysql.COMMENT_PREFIX {
			nodeName := strings.Trim(tokens[0], mysql.COMMENT_STRING)
			if c.schema.nodes[nodeName] != nil {
				executeDB.ExecNode = c.schema.nodes[nodeName]
			}
		}
	}

	if executeDB.ExecNode == nil {
		executeDB, err = c.GetExecDB(tokens, sql)
		if err != nil {
			return nil, err
		}
		if executeDB == nil {
			return nil, nil
		}
		return executeDB, nil
	}
	if len(c.txConns) == 1 && c.txConns[executeDB.ExecNode] == nil {
		return nil, errors.ErrTransInMulti
	}
	return executeDB, nil
}

func (c *ClientConn) GetExecDB(tokens []string, sql string) (*ExecuteDB, error) {

	tokensLen := len(tokens)
	return c.getSelectExecDB(sql, tokens, tokensLen)
}

func (c *ClientConn) setExecuteNode(tokens []string, tokensLen int, executeDB *ExecuteDB) error {
	if 2 <= tokensLen {
		//for /*node1*/
		if 1 < len(tokens) && tokens[0][0] == mysql.COMMENT_PREFIX {
			nodeName := strings.Trim(tokens[0], mysql.COMMENT_STRING)
			if c.schema.nodes[nodeName] != nil {
				executeDB.ExecNode = c.schema.nodes[nodeName]
			}
			//for /*node1*/ select
			if strings.ToLower(tokens[1]) == mysql.TK_STR_SELECT {
				executeDB.IsSlave = true
			}
		}
	}

	return nil
}

//get the execute database for select sql
func (c *ClientConn) getSelectExecDB(sql string, tokens []string, tokensLen int) (*ExecuteDB, error) {
	executeDB := new(ExecuteDB)
	executeDB.sql = sql
	executeDB.IsSlave = true

	err := c.setExecuteNode(tokens, tokensLen, executeDB)
	if err != nil {
		return nil, err
	}
	return executeDB, nil
}
