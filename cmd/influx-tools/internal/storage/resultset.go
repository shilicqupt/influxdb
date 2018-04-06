package storage

import (
	"context"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

type readRequest struct {
	ctx        context.Context
	start, end int64
	asc        bool
	limit      uint64
}

type ResultSet struct {
	req readRequest
	cur seriesCursor
	row seriesRow
}

func (r *ResultSet) Close() {
	r.row.query = nil
	r.cur.Close()
}

func (r *ResultSet) Next() bool {
	row := r.cur.Next()
	if row == nil {
		return false
	}

	r.row = *row

	return true
}

func (r *ResultSet) Cursor() tsdb.Cursor { return newMultiShardBatchCursor(r.req.ctx, r.row, &r.req) }
func (r *ResultSet) Name() []byte        { return r.row.name }
func (r *ResultSet) Tags() models.Tags   { return r.row.tags }
func (r *ResultSet) Field() []byte       { return []byte(r.row.field) }
