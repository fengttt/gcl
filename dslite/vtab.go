package dslite

import (
	"fmt"
	"github.com/mattn/go-sqlite3"
)

type ColumnInfo struct {
	Name string
	Typ  string
}

type VTCursor interface {
	Rowset() VTRowset
	Rewind() error
	Close() error
	Next() error
	Eof() bool
	Rowid() int64
	ScanBool(int) (bool, bool)
	ScanI64(int) (int64, bool)
	ScanInt(int) (int, bool)
	ScanF64(int) (float64, bool)
	ScanStr(int) (string, bool)
}

type VTRowset interface {
	Cursor() (VTCursor, error)
	Columns() ([]ColumnInfo, error)
}

type vtabModule struct {
	rowSets map[string]VTRowset
}

var g_vtabModule *vtabModule

func VtabFactory() *vtabModule {
	if g_vtabModule == nil {
		g_vtabModule = &vtabModule{
			rowSets: make(map[string]VTRowset),
		}
	}
	return g_vtabModule
}

func RegisterRowset(name string, rs VTRowset) error {
	vtm := VtabFactory()
	if rs == nil {
		delete(vtm.rowSets, name)
	} else {
		_, ok := vtm.rowSets[name]
		if ok {
			return fmt.Errorf("%s has not been registered", name)
		}
		vtm.rowSets[name] = rs
	}
	return nil
}

type vtabTab struct {
	rs VTRowset
}

type vtabCursor struct {
	cur VTCursor
}

func (vt *vtabTab) Open() (sqlite3.VTabCursor, error) {
	cur, err := vt.rs.Cursor()
	if err != nil {
		return nil, err
	}

	return &vtabCursor{cur}, nil
}

func (vt *vtabTab) BestIndex(cst []sqlite3.InfoConstraint, ob []sqlite3.InfoOrderBy) (*sqlite3.IndexResult, error) {
	return &sqlite3.IndexResult{}, nil
}

func (vt *vtabTab) Disconnect() error { return nil }
func (vt *vtabTab) Destroy() error    { return nil }

func (vtc *vtabCursor) Column(c *sqlite3.SQLiteContext, coln int) error {
	cols, err := vtc.cur.Rowset().Columns()
	if err != nil {
		return err
	}

	col := cols[coln]
	switch col.Typ {
	case "BOOL":
		b, ok := vtc.cur.ScanBool(coln)
		if ok {
			c.ResultBool(b)
		} else {
			c.ResultNull()
		}
	case "INT":
		i32, ok := vtc.cur.ScanInt(coln)
		if ok {
			c.ResultInt(i32)
		} else {
			c.ResultNull()
		}
	case "BIGINT":
		i64, ok := vtc.cur.ScanI64(coln)
		if ok {
			c.ResultInt64(i64)
		} else {
			c.ResultNull()
		}
	case "REAL":
		f64, ok := vtc.cur.ScanF64(coln)
		if ok {
			c.ResultDouble(f64)
		} else {
			c.ResultNull()
		}
	default:
		s, ok := vtc.cur.ScanStr(coln)
		if ok {
			c.ResultText(s)
		} else {
			c.ResultNull()
		}
	}
	return nil
}

func (vtc *vtabCursor) Filter(idxNum int, idxStr string, vals []interface{}) error {
	return vtc.cur.Rewind()
}

func (vtc *vtabCursor) Next() error {
	return vtc.cur.Next()
}

func (vtc *vtabCursor) EOF() bool {
	return vtc.cur.Eof()
}

func (vtc *vtabCursor) Rowid() (int64, error) {
	return vtc.cur.Rowid(), nil
}

func (vtc *vtabCursor) Close() error {
	return vtc.cur.Close()
}

func (m *vtabModule) Create(c *sqlite3.SQLiteConn, args []string) (sqlite3.VTab, error) {
	rsname := args[2]
	if len(args) == 4 {
		rsname = args[3]
	}

	rs, ok := m.rowSets[rsname]
	if !ok {
		return nil, fmt.Errorf("%s is not a regiesterd vtable, args %v", rsname, args)
	}

	cols, err := rs.Columns()
	if err != nil {
		return nil, fmt.Errorf("Cannot get schema of %s", args[0])
	}

	ddl := fmt.Sprintf("CREATE TABLE %s (", rsname)
	sep := ""
	for _, col := range cols {
		ddl = ddl + fmt.Sprintf("%s %s %s", sep, col.Name, col.Typ)
		sep = ", "
	}
	ddl = ddl + ")"

	if err := c.DeclareVTab(ddl); err != nil {
		return nil, err
	}

	return &vtabTab{rs}, nil
}

func (m *vtabModule) Connect(c *sqlite3.SQLiteConn, args []string) (sqlite3.VTab, error) {
	return m.Create(c, args)
}

func (m *vtabModule) DestroyModule() {
}

type xSlice interface {
	GetBool(int) (bool, bool)
	GetInt(int) (int, bool)
	GetI64(int) (int64, bool)
	GetF64(int) (float64, bool)
	GetStr(int) (string, bool)
}

type BoolSlice struct {
	n []bool
	v []bool
}

func (s *BoolSlice) GetBool(idx int) (bool, bool) {
	if idx >= len(s.v) {
		return false, false
	}
	if idx < len(s.n) {
		return s.v[idx], !s.n[idx]
	}
	return s.v[idx], true
}

func (s *BoolSlice) GetInt(idx int) (int, bool)     { return 0, false }
func (s *BoolSlice) GetI64(idx int) (int64, bool)   { return 0, false }
func (s *BoolSlice) GetF64(idx int) (float64, bool) { return 0, false }
func (s *BoolSlice) GetStr(idx int) (string, bool)  { return "", false }

type IntSlice struct {
	n []bool
	v []int
}

func (s *IntSlice) GetBool(idx int) (bool, bool) { return false, false }
func (s *IntSlice) GetInt(idx int) (int, bool) {
	if idx >= len(s.v) {
		return 0, false
	}
	if idx < len(s.n) {
		return s.v[idx], !s.n[idx]
	}
	return s.v[idx], true
}
func (s *IntSlice) GetI64(idx int) (int64, bool)   { return 0, false }
func (s *IntSlice) GetF64(idx int) (float64, bool) { return 0, false }
func (s *IntSlice) GetStr(idx int) (string, bool)  { return "", false }

type I64Slice struct {
	n []bool
	v []int64
}

func (s *I64Slice) GetBool(idx int) (bool, bool) { return false, false }
func (s *I64Slice) GetInt(idx int) (int, bool)   { return 0, false }
func (s *I64Slice) GetI64(idx int) (int64, bool) {
	if idx >= len(s.v) {
		return 0, false
	}
	if idx < len(s.n) {
		return s.v[idx], !s.n[idx]
	}
	return s.v[idx], true
}
func (s *I64Slice) GetF64(idx int) (float64, bool) { return 0, false }
func (s *I64Slice) GetStr(idx int) (string, bool)  { return "", false }

type F64Slice struct {
	n []bool
	v []float64
}

func (s *F64Slice) GetBool(idx int) (bool, bool) { return false, false }
func (s *F64Slice) GetInt(idx int) (int, bool)   { return 0, false }
func (s *F64Slice) GetI64(idx int) (int64, bool) { return 0, false }
func (s *F64Slice) GetF64(idx int) (float64, bool) {
	if idx >= len(s.v) {
		return 0, false
	}
	if idx < len(s.n) {
		return s.v[idx], !s.n[idx]
	}
	return s.v[idx], true
}
func (s *F64Slice) GetStr(idx int) (string, bool) { return "", false }

type StrSlice struct {
	n []bool
	v []string
}

func (s *StrSlice) GetBool(idx int) (bool, bool)   { return false, false }
func (s *StrSlice) GetInt(idx int) (int, bool)     { return 0, false }
func (s *StrSlice) GetI64(idx int) (int64, bool)   { return 0, false }
func (s *StrSlice) GetF64(idx int) (float64, bool) { return 0, false }
func (s *StrSlice) GetStr(idx int) (string, bool) {
	if idx >= len(s.v) {
		return "", false
	}
	if idx < len(s.n) {
		return s.v[idx], !s.n[idx]
	}
	return s.v[idx], true
}

type SliceRowset struct {
	Cols []ColumnInfo
	Data []xSlice
	Sz   int
}

type SliceCursor struct {
	rs  *SliceRowset
	idx int
}

func (s *SliceRowset) Columns() ([]ColumnInfo, error) { return s.Cols, nil }
func (s *SliceRowset) Cursor() (VTCursor, error) {
	cur := &SliceCursor{
		rs:  s,
		idx: 0,
	}
	return cur, nil
}

func (s *SliceRowset) AddBoolCol(name string, v []bool, n []bool) *SliceRowset {
	s.Cols = append(s.Cols, ColumnInfo{name, "BOOL"})
	s.Data = append(s.Data, &BoolSlice{n, v})
	if s.Sz < len(v) {
		s.Sz = len(v)
	}
	return s
}
func (s *SliceRowset) AddIntCol(name string, v []int, n []bool) *SliceRowset {
	s.Cols = append(s.Cols, ColumnInfo{name, "INT"})
	s.Data = append(s.Data, &IntSlice{n, v})
	if s.Sz < len(v) {
		s.Sz = len(v)
	}
	return s
}
func (s *SliceRowset) AddI64Col(name string, v []int64, n []bool) *SliceRowset {
	s.Cols = append(s.Cols, ColumnInfo{name, "BIGINT"})
	s.Data = append(s.Data, &I64Slice{n, v})
	if s.Sz < len(v) {
		s.Sz = len(v)
	}
	return s
}
func (s *SliceRowset) AddF64Col(name string, v []float64, n []bool) *SliceRowset {
	s.Cols = append(s.Cols, ColumnInfo{name, "REAL"})
	s.Data = append(s.Data, &F64Slice{n, v})
	if s.Sz < len(v) {
		s.Sz = len(v)
	}
	return s
}
func (s *SliceRowset) AddStrCol(name string, v []string, n []bool) *SliceRowset {
	s.Cols = append(s.Cols, ColumnInfo{name, "TEXT"})
	s.Data = append(s.Data, &StrSlice{n, v})
	if s.Sz < len(v) {
		s.Sz = len(v)
	}
	return s
}

func (c *SliceCursor) Rowset() VTRowset                { return c.rs }
func (c *SliceCursor) Rewind() error                   { c.idx = 0; return nil }
func (c *SliceCursor) Close() error                    { c.idx = 0; return nil }
func (c *SliceCursor) Next() error                     { c.idx += 1; return nil }
func (c *SliceCursor) Eof() bool                       { return c.idx >= c.rs.Sz }
func (c *SliceCursor) Rowid() int64                    { return int64(c.idx) }
func (c *SliceCursor) ScanBool(col int) (bool, bool)   { return c.rs.Data[col].GetBool(c.idx) }
func (c *SliceCursor) ScanInt(col int) (int, bool)     { return c.rs.Data[col].GetInt(c.idx) }
func (c *SliceCursor) ScanI64(col int) (int64, bool)   { return c.rs.Data[col].GetI64(c.idx) }
func (c *SliceCursor) ScanF64(col int) (float64, bool) { return c.rs.Data[col].GetF64(c.idx) }
func (c *SliceCursor) ScanStr(col int) (string, bool)  { return c.rs.Data[col].GetStr(c.idx) }
