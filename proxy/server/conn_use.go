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
	"github.com/flike/kingshard/backend"
	"github.com/flike/kingshard/core/hack"
)

func (c *ClientConn) handleUseDB(dbName string) error {
	var co *backend.BackendConn
	var err error

	if len(dbName) == 0 {
		return fmt.Errorf("must have database, the length of dbName is zero")
	}

	//co = GetConnection()
	svr := GetServer()
	co,err = c.getBackendConn(svr,false)
	if err != nil {
		hack.Yell("Failed")
		return err
	}
	if err = co.UseDB(dbName); err != nil {
		//reset the client database to null
		c.db = ""
		return err
	}
	c.db = dbName
	return c.writeOK(nil)
}
