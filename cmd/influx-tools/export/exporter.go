package export

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/storage"
	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"go.uber.org/zap"
)

type MetaClient interface {
	Database(name string) *meta.DatabaseInfo
	RetentionPolicy(database, name string) (rpi *meta.RetentionPolicyInfo, err error)
	// TODO(sgc): MUST return shards owned by this node only
	ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error)
}

type Config struct {
	Database string
	RP       string
	Data     tsdb.Config
}

type Exporter struct {
	MetaClient MetaClient
	TSDBStore  *tsdb.Store
	Store      *storage.Store

	db, rp string
}

func NewExporter(client MetaClient, cfg *Config, log *zap.Logger) (*Exporter, error) {
	dbi := client.Database(cfg.Database)
	if dbi == nil {
		return nil, fmt.Errorf("database '%s' does not exist", cfg.Database)
	}

	e := new(Exporter)

	e.MetaClient = client
	e.TSDBStore = tsdb.NewStore(cfg.Data.Dir)
	e.TSDBStore.EngineOptions.Config = cfg.Data
	e.TSDBStore.EngineOptions.EngineVersion = cfg.Data.Engine
	e.TSDBStore.EngineOptions.IndexVersion = cfg.Data.Index

	e.TSDBStore.EngineOptions.DatabaseFilter = func(database string) bool {
		return database == cfg.Database
	}

	if cfg.RP == "" {
		// select default RP
		cfg.RP = dbi.DefaultRetentionPolicy
	}

	rpi, err := client.RetentionPolicy(cfg.Database, cfg.RP)
	if err != nil {
		return nil, fmt.Errorf("retention policy '%s' does not exist", cfg.RP)
	}

	if rpi == nil {
		return nil, fmt.Errorf("retention policy '%s' does not exist", cfg.RP)
	}

	e.TSDBStore.EngineOptions.RetentionPolicyFilter = func(_, rp string) bool {
		return rp == cfg.RP
	}

	// open no shards to begin with
	e.TSDBStore.EngineOptions.ShardFilter = func(_, _ string, _ uint64) bool {
		return false
	}

	e.TSDBStore.WithLogger(log)

	e.Store = &storage.Store{TSDBStore: e.TSDBStore}

	e.db = cfg.Database
	e.rp = cfg.RP

	return e, nil
}

func (e *Exporter) Open() error {
	return e.TSDBStore.Open()
}

// Read creates a ResultSet that reads all points with a timestamp ts, such that start ≤ ts < end.
func (e *Exporter) Read(start, end int64) (*storage.ResultSet, error) {
	shards, err := e.getShards(start, end)
	if err != nil {
		return nil, err
	}

	req := storage.ReadRequest{
		Database:    e.db,
		RP:          e.rp,
		Shards:      shards,
		Start:       start,
		End:         end - 1,
		PointsLimit: math.MaxUint64,
	}

	return e.Store.Read(context.Background(), &req)
}

func (e *Exporter) Close() error {
	return e.TSDBStore.Close()
}

func (e *Exporter) getShards(start int64, end int64) ([]*tsdb.Shard, error) {
	groups, err := e.MetaClient.ShardGroupsByTimeRange(e.db, e.rp, time.Unix(0, start), time.Unix(0, end))
	if err != nil {
		return nil, err
	}

	if len(groups) == 0 {
		return nil, nil
	}

	sort.Sort(meta.ShardGroupInfos(groups))

	var ids []uint64
	for _, g := range groups {
		for _, s := range g.Shards {
			ids = append(ids, s.ID)
		}
	}

	shards := e.TSDBStore.Shards(ids)
	if len(shards) == len(ids) {
		return shards, nil
	}

	return e.openStoreWithShardsIDs(ids)
}
func (e *Exporter) openStoreWithShardsIDs(ids []uint64) ([]*tsdb.Shard, error) {
	e.TSDBStore.Close()
	e.TSDBStore.EngineOptions.ShardFilter = func(_, _ string, id uint64) bool {
		for i := range ids {
			if id == ids[i] {
				return true
			}
		}
		return false
	}
	if err := e.TSDBStore.Open(); err != nil {
		return nil, err
	}
	return e.TSDBStore.Shards(ids), nil
}
