package export

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format"
	"github.com/influxdata/influxdb/cmd/influx-tools/internal/format/binary"
	"github.com/influxdata/influxdb/cmd/influxd/run"
	"github.com/influxdata/influxdb/logger"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/meta"
	"go.uber.org/zap"
)

// Command represents the program execution for "store query".
type Command struct {
	// Standard input/output, overridden for testing.
	Stderr io.Writer
	Stdout io.Writer
	Logger *zap.Logger

	configPath      string
	cpuProfile      string
	memProfile      string
	database        string
	rp              string
	shardDuration   time.Duration
	retentionPolicy string
	startTime       int64
	endTime         int64
	limit           uint64
	slimit          uint64
	soffset         uint64
	expr            string
}

// NewCommand returns a new instance of Command.
func NewCommand() *Command {
	return &Command{
		Stderr: os.Stderr,
		Stdout: os.Stdout,
	}
}

func (cmd *Command) Run(args []string) (err error) {
	err = cmd.parseFlags(args)
	if err != nil {
		return err
	}

	config, err := cmd.ParseConfig(cmd.getConfigPath())
	if err != nil {
		return fmt.Errorf("parse config: %s", err)
	}

	// Validate the configuration.
	if err = config.Validate(); err != nil {
		return fmt.Errorf("validate config: %s", err)
	}

	var logErr error
	if cmd.Logger, logErr = config.Logging.New(cmd.Stderr); logErr != nil {
		// assign the default logger
		cmd.Logger = logger.New(cmd.Stderr)
	}

	e, err := cmd.openExporter(config, cmd.Logger)
	if err != nil {
		return err
	}
	defer e.Close()

	rs, err := e.Read(models.MinNanoTime, models.MaxNanoTime)
	if err != nil || rs == nil {
		return err
	}
	defer rs.Close()

	wr := binary.NewWriter(os.Stdout, cmd.database, cmd.rp, cmd.shardDuration)
	format.WriteBucket(wr, models.MinNanoTime, models.MaxNanoTime, rs)
	wr.WriteStats(os.Stderr)

	return wr.Close()
}

func (cmd *Command) openExporter(config *run.Config, log *zap.Logger) (*Exporter, error) {
	client := meta.NewClient(config.Meta)
	client.WithLogger(log)
	if err := client.Open(); err != nil {
		return nil, err
	}

	cfg := &Config{Database: cmd.database, RP: cmd.rp, Data: config.Data}
	e, err := NewExporter(client, cfg, log)
	if err != nil {
		client.Close()
		return nil, err
	}

	return e, e.Open()
}

func (cmd *Command) parseFlags(args []string) error {
	fs := flag.NewFlagSet("export", flag.ContinueOnError)
	fs.StringVar(&cmd.configPath, "config", "", "Config file")
	fs.StringVar(&cmd.database, "database", "", "Database name")
	fs.StringVar(&cmd.rp, "rp", "", "Retention policy name")
	fs.DurationVar(&cmd.shardDuration, "duration", time.Hour*24*7, "Target shard duration")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if cmd.database == "" {
		return errors.New("database is required")
	}

	return nil
}

// ParseConfig parses the config at path.
// It returns a demo configuration if path is blank.
func (cmd *Command) ParseConfig(path string) (*run.Config, error) {
	// Use demo configuration if no config path is specified.
	if path == "" {
		return nil, errors.New("missing config file")
	}

	config := run.NewConfig()
	if err := config.FromTomlFile(path); err != nil {
		return nil, err
	}

	return config, nil
}

// GetConfigPath returns the config path from the options.
// It will return a path by searching in this order:
//   1. The CLI option in ConfigPath
//   2. The environment variable INFLUXDB_CONFIG_PATH
//   3. The first influxdb.conf file on the path:
//        - ~/.influxdb
//        - /etc/influxdb
func (cmd *Command) getConfigPath() string {
	if cmd.configPath != "" {
		if cmd.configPath == os.DevNull {
			return ""
		}
		return cmd.configPath
	}

	for _, path := range []string{
		os.ExpandEnv("${HOME}/.influxdb/influxdb.conf"),
		"/etc/influxdb/influxdb.conf",
	} {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return ""
}
