package parser

import (
	"strings"

	"github.com/XiaohanLiang/kingshard/lib/errors"
	"github.com/xwb1989/sqlparser"
)

func Parse(sql string) (action string, tables []string, databases []string, err error) {

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
