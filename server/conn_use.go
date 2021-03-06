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
)

func (c *ClientConn) handleUseDB(dbName string) error {

	if len(dbName) == 0 {
		return fmt.Errorf("must have database, the length of dbName is zero")
	}

	conn, err := c.getBackendConn(GetServer(), false)
	if err != nil {
		return err
	}
	if err = conn.UseDB(dbName); err != nil {
		c.db = ""
		return err
	}
	c.db = dbName
	return c.writeOK(nil)
}
