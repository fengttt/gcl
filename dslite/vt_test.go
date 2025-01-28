package dslite

import (
	"fmt"
	"log"
	"testing"
)

func TestVt(t *testing.T) {
	db, err := OpenDB(":memory:")
	if err != nil {
		log.Panic("Cannot open database", err)
	}
	defer db.Close()

	var x8 SliceRowset
	x8s := []int{1, 2, 3, 4, 5, 6, 7, 8}
	x8.AddIntCol("value", x8s, nil)
	RegisterRowset("x8", &x8)

	_, err = db.Exec("create virtual table x8 using govt(x8)")
	if err != nil {
		log.Panic("Error: ", err)
	}

	qry := `
		select * from x8 r1, x8 r2, x8 r3, x8 r4,
		              x8 r5, x8 r6, x8 r7, x8 r8
		where 
			r2.value - r1.value != 0 and r2.value - r1.value != 1 and r2.value - r1.value != -1
			and r3.value - r1.value != 0 and r3.value - r1.value != 2 and r3.value - r1.value != -2
			and r4.value - r1.value != 0 and r4.value - r1.value != 3 and r4.value - r1.value != -3
			and r5.value - r1.value != 0 and r5.value - r1.value != 4 and r5.value - r1.value != -4
			and r6.value - r1.value != 0 and r6.value - r1.value != 5 and r6.value - r1.value != -5
			and r7.value - r1.value != 0 and r7.value - r1.value != 6 and r7.value - r1.value != -6
			and r8.value - r1.value != 0 and r8.value - r1.value != 7 and r8.value - r1.value != -7

			and r3.value - r2.value != 0 and r3.value - r2.value != 1 and r3.value - r2.value != -1
			and r4.value - r2.value != 0 and r4.value - r2.value != 2 and r4.value - r2.value != -2
			and r5.value - r2.value != 0 and r5.value - r2.value != 3 and r5.value - r2.value != -3
			and r6.value - r2.value != 0 and r6.value - r2.value != 4 and r6.value - r2.value != -4
			and r7.value - r2.value != 0 and r7.value - r2.value != 5 and r7.value - r2.value != -5
			and r8.value - r2.value != 0 and r8.value - r2.value != 6 and r8.value - r2.value != -6

			and r4.value - r3.value != 0 and r4.value - r3.value != 1 and r4.value - r3.value != -1
			and r5.value - r3.value != 0 and r5.value - r3.value != 2 and r5.value - r3.value != -2
			and r6.value - r3.value != 0 and r6.value - r3.value != 3 and r6.value - r3.value != -3
			and r7.value - r3.value != 0 and r7.value - r3.value != 4 and r7.value - r3.value != -4
			and r8.value - r3.value != 0 and r8.value - r3.value != 5 and r8.value - r3.value != -5

			and r5.value - r4.value != 0 and r5.value - r4.value != 1 and r5.value - r4.value != -1
			and r6.value - r4.value != 0 and r6.value - r4.value != 2 and r6.value - r4.value != -2
			and r7.value - r4.value != 0 and r7.value - r4.value != 3 and r7.value - r4.value != -3
			and r8.value - r4.value != 0 and r8.value - r4.value != 4 and r8.value - r4.value != -4

			and r6.value - r5.value != 0 and r6.value - r5.value != 1 and r6.value - r5.value != -1
			and r7.value - r5.value != 0 and r7.value - r5.value != 2 and r7.value - r5.value != -2
			and r8.value - r5.value != 0 and r8.value - r5.value != 3 and r8.value - r5.value != -3

			and r7.value - r6.value != 0 and r7.value - r6.value != 1 and r7.value - r6.value != -1
			and r8.value - r6.value != 0 and r8.value - r6.value != 2 and r8.value - r6.value != -2

			and r8.value - r7.value != 0 and r8.value - r7.value != 1 and r8.value - r7.value != -1
		`

	rows, err := db.Query(qry)
	if err != nil {
		log.Panic("Error: ", err)
	}

	cnt := 0

	for rows.Next() {
		var r1, r2, r3, r4, r5, r6, r7, r8 int
		err = rows.Scan(&r1, &r2, &r3, &r4, &r5, &r6, &r7, &r8)
		if err != nil {
			log.Panic("Cannot scan rows", err)
		}
		cnt += 1
		fmt.Printf("Row: %d (%d, %d, %d, %d, %d, %d, %d, %d)\n", cnt, r1, r2, r3, r4, r5, r6, r7, r8)
	}
}

func Test2Cols(t *testing.T) {
	db, err := OpenDB(":memory:")
	if err != nil {
		log.Panic("Cannot open database", err)
	}
	defer db.Close()

	var x8 SliceRowset
	x8i := []int{1, 2, 3, 4, 5, 6, 7, 8}
	x8.AddIntCol("i", x8i, nil)
	x8s := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	x8.AddStrCol("s", x8s, nil)
	RegisterRowset("test2col", &x8)

	_, err = db.Exec("create virtual table test2col using govt(test2col)")
	if err != nil {
		log.Panic("Error: ", err)
	}

	qry := `select i, s from test2col`
	rows, err := db.Query(qry)
	if err != nil {
		log.Panic("Error: ", err)
	}
	for rows.Next() {
		var i int
		var s string
		err = rows.Scan(&i, &s)
		if err != nil {
			log.Panic("Cannot scan rows", err)
		}
		fmt.Printf("Row: (%d, %s)\n", i, s)
	}

	fmt.Println(PrintQuery(db, qry))
}
