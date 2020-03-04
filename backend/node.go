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

package backend

import (
	"github.com/flike/kingshard/core/hack"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flike/kingshard/core/errors"
	"github.com/flike/kingshard/core/golog"
)

const (
	Master      = "master"
	Slave       = "slave"
	SlaveSplit  = ","
	WeightSplit = "@"
)

type Node struct {

	sync.RWMutex
	Master *DB

	Slave          []*DB
	LastSlaveIndex int
	RoundRobinQ    []int
	SlaveWeights   []int

	DownAfterNoAlive time.Duration

	Online bool
}

func (n *Node) CheckNode() {
	for n.Online {
		n.checkMaster()
		time.Sleep(16 * time.Second)
	}
}

func (n *Node) String() string {
	return n.Master.db
}

func (n *Node) GetMasterConn() (*BackendConn, error) {
	db := n.Master
	if db == nil {
		return nil, errors.ErrNoMasterConn
	}
	if atomic.LoadInt32(&(db.state)) == Down {
		return nil, errors.ErrMasterDown
	}
	return db.GetConn()
}

func (n *Node) GetSlaveConn() (*BackendConn, error) {
	n.Lock()
	db, err := n.GetNextSlave()
	n.Unlock()
	if err != nil {
		return nil, err
	}

	if db == nil {
		return nil, errors.ErrNoSlaveDB
	}
	if atomic.LoadInt32(&(db.state)) == Down {
		return nil, errors.ErrSlaveDown
	}

	return db.GetConn()
}

func (n *Node) checkMaster() {
	db := n.Master
	if db == nil {
		golog.Error("Node", "checkMaster", "Master is no alive", 0)
		return
	}

	if err := db.Ping(); err != nil {
		golog.Error("Node", "checkMaster", "Ping", 0, "db.Addr", db.Addr(), "error", err.Error())
	} else {
		if atomic.LoadInt32(&(db.state)) == Down {
			golog.Info("Node", "checkMaster", "Master up", 0, "db.Addr", db.Addr())
			err := n.UpMaster(db.addr)
			if err != nil {
				golog.Error("Node", "checkMaster", "UpMaster", 0, "db.Addr", db.Addr(), "error", err.Error())
				return
			}
		}
		db.SetLastPing()
		if atomic.LoadInt32(&(db.state)) != ManualDown {
			atomic.StoreInt32(&(db.state), Up)
		}
		return
	}

	if int64(n.DownAfterNoAlive) > 0 && time.Now().Unix()-db.GetLastPing() > int64(n.DownAfterNoAlive/time.Second) {
		golog.Info("Node", "checkMaster", "Master down", 0,
			"db.Addr", db.Addr(),
			"Master_down_time", int64(n.DownAfterNoAlive/time.Second))
		n.DownMaster(db.addr, Down)
	}
}

func (n *Node) OpenDB() (*DB, error) {
	db, err := Open(n.Master.addr, n.Master.user, n.Master.password, "", 0)
	return db, err
}

func (n *Node) UpDB() (*DB, error) {
	db, err := n.OpenDB()

	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		db.Close()
		atomic.StoreInt32(&(db.state), Down)
		return nil, err
	}
	atomic.StoreInt32(&(db.state), Up)
	return db, nil
}

func (n *Node) UpMaster(addr string) error {
	db, err := n.UpDB()
	if err != nil {
		golog.Error("Node", "UpMaster", err.Error(), 0)
		return err
	}
	n.Master = db
	return err
}

func (n *Node) DownMaster(addr string, state int32) error {
	db := n.Master
	if db == nil || db.addr != addr {
		return errors.ErrNoMasterDB
	}

	db.Close()
	atomic.StoreInt32(&(db.state), state)
	return nil
}
