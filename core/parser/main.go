package parser

import (
	"fmt"
	"github.com/flike/kingshard/core/errors"
	"github.com/xwb1989/sqlparser"
	"strings"
)

var (
	_actions   = []string{"truncate"}
	_tables    = []string{"secret"}
	_databases = []string{"secret"}
)


func Parse(sql string) error {

	subQueries := strings.Split(sql, ";")
	for _,v := range subQueries {
		action,tables,databases,err := parse(v)
		if err != nil {
			return err
		}
		if v,ok := contains(_actions,[]string{action}); ok {
			return fmt.Errorf("%s 操作被禁止 \n",v)
		}
		if v,ok := contains(_tables,tables); ok {
			return fmt.Errorf("%s 表禁止访问 \n",v)
		}
		if v,ok := contains(_databases,databases); ok {
			return fmt.Errorf("%s 库禁止访问 \n",v)
		}
	}

	return nil
}

func parse(sql string) (action string, tables []string, databases []string, err error) {

	var (
		stmt sqlparser.Statement
	)

	stmt, err = sqlparser.Parse(sql)
	if err != nil {
		return
	}

	switch stmt.(type) {
	case *sqlparser.Select:
		action = "Select"
	case *sqlparser.Update:
		action = "Update"
	case *sqlparser.Delete:
		action = "Delete"
	case *sqlparser.Set:
		action = "Set"
	case *sqlparser.Use:
		action = "Use"
	case *sqlparser.Begin:
		action = "Begin"
	case *sqlparser.Commit:
		action = "Commit"
	case *sqlparser.Rollback:
		action = "Rollback"
	case *sqlparser.Insert:
		action = stmt.(*sqlparser.Insert).Action
	case *sqlparser.DDL:
		action = stmt.(*sqlparser.DDL).Action
	default:
		err = errors.ErrCmdUnsupport
	}

	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case sqlparser.TableName:
			if node.IsEmpty() {
				return
			}
			databases = append(tables, node.Qualifier.CompliantName())
			tables = append(tables, node.Name.CompliantName())
		}
		return true, nil
	}, stmt)

	return
}

func contains(a []string, b []string) (string, bool) {
	for _, v := range a {
		for _, vv := range b {
			if strings.ToUpper(v) == strings.ToUpper(vv) {
				return v, true
			}
		}
	}
	return "", false
}
