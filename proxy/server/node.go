package server

import (
	"github.com/flike/kingshard/backend"
	"github.com/flike/kingshard/config"
	"github.com/flike/kingshard/core/errors"
)

var (
	svr *backend.Node = nil
	conn *backend.BackendConn = nil
)

func GetConnection() *backend.BackendConn {

	if conn != nil {
		return conn
	}
	co,err := GetServer().Master.GetConn()
	errors.Check(err)
	conn = co
	return conn
}

func GetServer() *backend.Node {
	if svr != nil {
		return svr
	}

	db, err := backend.Open(config.Addr, config.User, config.Password, "", 0)
	errors.Check(err)
	svr := &backend.Node{
		Master:           db,
		Online:           true,
	}
	go svr.CheckNode()
	return svr
}