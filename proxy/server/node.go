package server

import "github.com/flike/kingshard/backend"

func GetServerNode(me string) (*backend.Node,error) {
	svr := new(backend.Node)
	db, err := svr.OpenDB(me)
	if err != nil {
		return nil,err
	}
	svr.Master = db
	svr.Online = true
	go svr.CheckNode()
	return svr,nil
}
