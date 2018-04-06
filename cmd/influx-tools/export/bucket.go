package export

import (
	"time"

	"github.com/influxdata/influxdb/services/meta"
)

type Query struct {
	Start          time.Time
	End            time.Time
	TargetDuration time.Duration
}

func (q *Query) Shards() meta.ShardGroupInfos {
	first := q.Start.Truncate(q.TargetDuration).UTC()
	last := q.End.Truncate(q.TargetDuration).Add(q.TargetDuration).UTC()

	start := first

	sgis := make(meta.ShardGroupInfos, last.Sub(first)/q.TargetDuration)
	var i uint64
	for start.Before(last) {
		sgis[i] = meta.ShardGroupInfo{
			ID:        i,
			StartTime: start,
			EndTime:   start.Add(q.TargetDuration),
		}
		i++
		start = start.Add(q.TargetDuration)
	}
	return sgis[:i]
}
