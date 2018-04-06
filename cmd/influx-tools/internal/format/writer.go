package format

import (
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/storage"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

type Writer interface {
	NewBucket(start, end int64) (BucketWriter, error)
	Close() error
}

type BucketWriter interface {
	Err() error
	WriteSeries(name, field []byte, tags models.Tags)
	WriteCursor(cur tsdb.Cursor)
	Close() error
}

func WriteBucket(rs *storage.ResultSet, w BucketWriter) error {
	for rs.Next() {
		cur := rs.Cursor()
		if cur == nil {
			// no data for series key + field combination
			continue
		}

		w.WriteSeries(rs.Name(), rs.Field(), rs.Tags())
		w.WriteCursor(cur)

		if w.Err() != nil {
			return w.Err()
		}
	}
	return nil
}
