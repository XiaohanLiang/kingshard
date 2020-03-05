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
	"net"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/XiaohanLiang/kingshard/mysql"

	"sync"

	"github.com/XiaohanLiang/kingshard/backend"
	"github.com/XiaohanLiang/kingshard/lib/golog"
)

type Schema struct {
	nodes map[string]*backend.Node
}

type BlacklistSqls struct {
	sqls    map[string]string
	sqlsLen int
}

const (
	Offline = iota
	Online
	Unknown
)

type Server struct {
	addr string

	statusIndex    int32
	status         [2]int32
	allowipsIndex  BoolIndex
	allowips       [2][]IPInfo
	blockedActions []string
	blockedTables  []string
	blockedDbs     []string
	StartTime      string
	EndTime        string

	counter *Counter

	listener net.Listener
	running  bool

	configUpdateMutex sync.RWMutex
	configVer         uint32
}

func NewServer(addr string) (*Server, error) {
	s := new(Server)

	s.counter = new(Counter)
	s.addr = addr
	atomic.StoreInt32(&s.statusIndex, 0)
	s.status[s.statusIndex] = Online
	s.configVer = 0

	// Key: you shouldn't set up any user specified config
	// They were supposed to be given when a new connection
	// is set up, after inspected etcd for user config :)
	//
	// Config = server config + user specified
	// Server Config = IpBlock + TimeBlock
	// User Config = Chart + Action + DbRestriction

	var err error
	netProto := "tcp"

	s.listener, err = net.Listen(netProto, s.addr)

	if err != nil {
		return nil, err
	}

	golog.Info("server", "NewServer", "Server running", 0,
		"netProto",
		netProto,
		"address",
		s.addr)
	return s, nil
}

func (s *Server) flushCounter() {
	for {
		s.counter.FlushCounter()
		time.Sleep(1 * time.Second)
	}
}

func (s *Server) newClientConn(co net.Conn) *ClientConn {
	c := new(ClientConn)
	tcpConn := co.(*net.TCPConn)

	//SetNoDelay controls whether the operating system should delay packet transmission
	// in hopes of sending fewer packets (Nagle's algorithm).
	// The default is true (no delay),
	// meaning that data is sent as soon as possible after a Write.
	//I set this option false.
	tcpConn.SetNoDelay(false)
	c.c = tcpConn

	func() {
		s.configUpdateMutex.RLock()
		defer s.configUpdateMutex.RUnlock()
		c.proxy = s
		c.configVer = s.configVer
	}()

	c.pkg = mysql.NewPacketIO(tcpConn)
	c.proxy = s

	c.pkg.Sequence = 0

	c.connectionId = atomic.AddUint32(&baseConnId, 1)

	c.status = mysql.SERVER_STATUS_AUTOCOMMIT

	c.salt, _ = mysql.RandomBuf(20)

	c.txConns = make(map[*backend.Node]*backend.BackendConn)

	c.closed = false

	c.charset = mysql.DEFAULT_CHARSET
	c.collation = mysql.DEFAULT_COLLATION_ID

	c.stmtId = 0

	return c
}

func (s *Server) onConn(c net.Conn) {
	s.counter.IncrClientConns()
	conn := s.newClientConn(c) //新建一个conn

	defer func() {
		err := recover()
		if err != nil {
			const size = 4096
			buf := make([]byte, size)
			buf = buf[:runtime.Stack(buf, false)] //获得当前goroutine的stacktrace
			golog.Error("server", "onConn", "error", 0,
				"remoteAddr", c.RemoteAddr().String(),
				"stack", string(buf),
			)
		}

		conn.Close()
		s.counter.DecrClientConns()
	}()

	// Step - 1 Read UserInfo from connection
	// Step - 2 Read Server Config (IpBlock + TimingBlock)
	// Step - 3 Read User Specified Info
	if allowConnect := conn.IsAllowConnect(); allowConnect == false {
		err := mysql.NewError(mysql.ER_ACCESS_DENIED_ERROR, "ip address access denied by kingshard.")
		conn.writeError(err)
		conn.Close()
		return
	}
	if err := conn.Handshake(); err != nil {
		golog.Error("server", "onConn", err.Error(), 0)
		conn.writeError(err)
		conn.Close()
		return
	}

	conn.Run()
}

func (s *Server) Run() error {
	s.running = true

	// flush counter
	go s.flushCounter()

	for s.running {
		conn, err := s.listener.Accept()
		if err != nil {
			golog.Error("server", "Run", err.Error(), 0)
			continue
		}

		go s.onConn(conn)
	}

	return nil
}

func (s *Server) Close() {
	s.running = false
	if s.listener != nil {
		s.listener.Close()
	}
}
