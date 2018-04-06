package export_test

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb/cmd/influx-tools/export"
	"github.com/influxdata/influxdb/services/meta"
)

func ts(s int64) time.Time { return time.Unix(s, 0).UTC() }

func ms(times ...int64) meta.ShardGroupInfos {
	sgis := make(meta.ShardGroupInfos, len(times)-1)
	for i := range sgis {
		sgis[i] = meta.ShardGroupInfo{ID: uint64(i), StartTime: ts(times[i]), EndTime: ts(times[i+1])}
	}
	return sgis
}

func TestQuery_Shards(t *testing.T) {
	type fields struct {
		Start          time.Time
		End            time.Time
		TargetDuration time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		exp    meta.ShardGroupInfos
	}{
		{
			fields: fields{
				Start:          ts(15),
				End:            ts(25),
				TargetDuration: 10 * time.Second,
			},
			exp: ms(10, 20, 30),
		},
		{
			fields: fields{
				Start:          ts(15),
				End:            ts(20),
				TargetDuration: 10 * time.Second,
			},
			exp: ms(10, 20, 30),
		},
		{
			fields: fields{
				Start:          ts(15),
				End:            ts(17),
				TargetDuration: 10 * time.Second,
			},
			exp: ms(10, 20),
		},
		{
			fields: fields{
				Start:          ts(10),
				End:            ts(20),
				TargetDuration: 10 * time.Second,
			},
			exp: ms(10, 20, 30),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &export.Query{
				Start:          tt.fields.Start,
				End:            tt.fields.End,
				TargetDuration: tt.fields.TargetDuration,
			}

			if got := q.Shards(); !cmp.Equal(got, tt.exp) {
				t.Errorf("unexpected value -got/+exp\n%s", cmp.Diff(got, tt.exp))
			}
		})
	}
}
