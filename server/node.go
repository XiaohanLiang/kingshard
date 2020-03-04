package server

import (
	"github.com/XiaohanLiang/kingshard/backend"
	"github.com/XiaohanLiang/kingshard/lib/errors"
)

var (
	svr      *backend.Node        = nil
	conn     *backend.BackendConn = nil
	Addr     string               = "127.0.0.1:3306"
	User     string               = "root"
	Password string               = "my-secret-pw"
)

func GetServer() *backend.Node {
	if svr != nil {
		return svr
	}

	db, err := backend.Open(Addr, User, Password, "", 0)
	errors.Check(err)
	svr = &backend.Node{
		Master: db,
		Online: true,
	}
	go svr.CheckNode()
	return svr
}
