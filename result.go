package drill

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/francoispqt/gojay"
)

type result struct {
	columns columns
	rows    *rows
}

func newResult(r io.Reader) (driver.Rows, error) {
	// FIXME: switch to make this a streaming result instead
	var theresult result
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if err := gojay.UnmarshalJSONObject(buf, &theresult); err != nil {
		return nil, err
	}
	return theresult.rows, nil
}

type columns struct {
	values []string
}

type rows struct {
	parent *result
	rows   []row
	index  int
}

var _ driver.Rows = (*rows)(nil)

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *rows) Columns() []string {
	return r.parent.columns.values
}

// Close closes the rows iterator.
func (r *rows) Close() error {
	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *rows) Next(dest []driver.Value) error {
	if r.index >= len(r.rows) {
		return sql.ErrNoRows
	}
	if len(dest) != len(r.parent.columns.values) {
		return fmt.Errorf("invalid scan, expected %d arguments and received %d", len(r.parent.columns.values), len(dest))
	}
	therow := r.rows[r.index]
	for i := 0; i < len(r.parent.columns.values); i++ {
		key := r.parent.columns.values[i]
		val := therow.kv[key]
		dest[i] = val
	}
	r.index++
	return nil
}

type row struct {
	kv map[string]interface{}
}

func (c *columns) UnmarshalJSONArray(dec *gojay.Decoder) error {
	str := ""
	if err := dec.String(&str); err != nil {
		return err
	}
	c.values = append(c.values, str)
	return nil
}

func (r *rows) UnmarshalJSONArray(dec *gojay.Decoder) error {
	var val row
	val.kv = make(map[string]interface{})
	if err := dec.Object(&val); err != nil {
		return err
	}
	r.rows = append(r.rows, val)
	return nil
}

func (r *row) UnmarshalJSONObject(dec *gojay.Decoder, key string) error {
	var obj interface{}
	if err := dec.Interface(&obj); err != nil {
		return err
	}
	r.kv[key] = obj
	return nil
}

func (r *row) NKeys() int {
	return 0
}

func (r *result) UnmarshalJSONObject(dec *gojay.Decoder, key string) error {
	switch key {
	case "columns":
		return dec.Array(&r.columns)
	case "rows":
		var therows rows
		therows.rows = make([]row, 0)
		if err := dec.Array(&therows); err != nil {
			return err
		}
		therows.parent = r
		r.rows = &therows
	}
	return nil
}

func (r *result) NKeys() int {
	return 2
}
