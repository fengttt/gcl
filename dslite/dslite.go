package dslite

import (
	"database/sql"
	"log"
	"strings"

	// vec "github.com/asg017/sqlite-vec-go-bindings/cgo"
	"github.com/mattn/go-sqlite3"
	"github.com/olekukonko/tablewriter"
)

func OpenDB(dsn string) (*sql.DB, error) {
	if dsn == "" || dsn == ":memory:" {
		dsn = "file:memory.db?cache=shared&mode=memory"
	}

	db, err := sql.Open("dslite3", dsn)
	return db, err
}

func init() {
	sql.Register("dslite3",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				err := conn.CreateModule("govt", VtabFactory())
				if err != nil {
					log.Panic("Cannot create govt module. ", err)
					return err
				}

				return nil
			},
		})

	// vec.Auto()
}

func QueryValue(db *sql.DB, qry string, args ...any) (interface{}, error) {
	rows, err := db.Query(qry, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}

	var val interface{}
	err = rows.Scan(&val)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func PrintQuery(db *sql.DB, qry string, args ...any) (string, error) {
	rows, err := db.Query(qry, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil || len(cols) == 0 {
		return "", err
	}

	ncol := len(cols)
	sb := &strings.Builder{}
	tw := tablewriter.NewWriter(sb)
	tw.SetHeader(cols)
	tw.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	tw.SetCenterSeparator("|")

	for rows.Next() {
		cells := make([]string, ncol)
		vals := make([]any, ncol)
		for i := 0; i < ncol; i++ {
			vals[i] = &cells[i]
		}
		if err = rows.Scan(vals...); err != nil {
			return "", err
		}
		tw.Append(cells)
	}
	tw.Render()
	return sb.String(), nil
}
