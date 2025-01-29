package dslite

import (
	"database/sql"
	"fmt"
	"log"
	"testing"

	"github.com/fengttt/gcl"
)

func TestJt(t *testing.T) {
	db, err := OpenDB("./test.db")
	if err != nil {
		t.Error("Cannot open database", err)
	}
	defer db.Close()

	ver, err := QueryValue(db, "select sqlite_version()")
	if err != nil {
		t.Error("Cannot query sqlite_version", err)
	}
	fmt.Println("SQLite version: ", ver)

	gcl.Must(db.Exec("drop table if exists jt"))
	gcl.Must(db.Exec("create table jt (i int, j text)"))
	gcl.Must(db.Exec(`insert into jt values (1, '{"a": 1, "b": 2}')`))
	gcl.Must(db.Exec(`insert into jt values (2, '{"a": "a"}')`))

	qry := `select i, json_extract(j, '$.a') as a, json_extract(j, '$.b') as b from jt`
	rows, err := db.Query(qry)
	if err != nil {
		log.Panic("Error: ", err)
	}

	for rows.Next() {
		var i int
		var s1 string
		var s2 sql.NullString
		err = rows.Scan(&i, &s1, &s2)
		if err != nil {
			log.Panic("Cannot scan rows", err)
		}
		fmt.Printf("Row: (%d, %s, %s)\n", i, s1, s2.String)
	}
}

/*
func TestVec(t *testing.T) {
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Error("Cannot open database", err)
	}
	defer db.Close()

	vv, err := QueryValue(db, "select vec_version()")
	if err != nil {
		t.Error("Cannot query vec_version", err)
	}
	fmt.Printf("Vec version: %v\n", vv)
}
*/
