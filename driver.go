package athena

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
)

// Error types
var (
	ErrConfigRequired    = errors.New("config is required")
	ErrDatabaseRequired  = errors.New("database is required")
	ErrSessionRequired   = errors.New("session is required")
	ErrInvalidResultMode = errors.New("invalid result mode")
)

var (
	openFromSessionMutex sync.Mutex
	openFromSessionCount int
)

const (
	// timeOutLimitDefault athena's timeout limit
	timeOutLimitDefault uint = 1800
)

// Driver is a sql.Driver. It's intended for db/sql.Open().
type Driver struct {
	cfg *Config
}

// NewDriver allows you to register your own driver with `sql.Register`.
// It's useful for more complex use cases. Read more in PR #3.
// https://github.com/segmentio/go-athena/pull/3
//
// Generally, sql.Open() or athena.Open() should suffice.
func NewDriver(cfg *Config) *Driver {
	return &Driver{cfg}
}

func init() {
	var drv driver.Driver = &Driver{}
	sql.Register("athena", drv)
}

// Open should be used via `db/sql.Open("athena", "<params>")`.
// The following parameters are supported in URI query format (k=v&k2=v2&...)
//
// - `db` (required)
// This is the Athena database name. In the UI, this defaults to "default",
// but the driver requires it regardless.
//
// - `output_location` (required)
// This is the S3 location Athena will dump query results in the format
// "s3://bucket/and/so/forth". In the AWS UI, this defaults to
// "s3://aws-athena-query-results-<ACCOUNTID>-<REGION>", but the driver requires it.
//
// - `poll_frequency` (optional)
// Athena's API requires polling to retrieve query results. This is the frequency at
// which the driver will poll for results. It should be a time/Duration.String().
// A completely arbitrary default of "5s" was chosen.
//
// - `region` (optional)
// Override AWS region. Useful if it is not set with environment variable.
//
// - `workgroup` (optional)
// Athena's workgroup. This defaults to "primary".
//
// Credentials must be accessible via the SDK's Default Credential Provider Chain.
// For more advanced AWS credentials/session/config management, please supply
// a custom AWS session directly via `athena.Open()`.
func (d *Driver) Open(connStr string) (driver.Conn, error) {
	cfg := d.cfg
	if cfg == nil {
		var err error
		cfg, err = configFromConnectionString(connStr)
		if err != nil {
			return nil, err
		}
	}

	if cfg.PollFrequency == 0 {
		cfg.PollFrequency = 5 * time.Second
	}

	// athena client
	athenaClient := athena.NewFromConfig(cfg.Config)

	// output location (with empty value)
	if checkOutputLocation(cfg.ResultMode, cfg.OutputLocation) {
		var err error
		cfg.OutputLocation, err = getOutputLocation(athenaClient, cfg.WorkGroup)
		if err != nil {
			return nil, err
		}
	}

	return &conn{
		athena:         athenaClient,
		db:             cfg.Database,
		OutputLocation: cfg.OutputLocation,
		pollFrequency:  cfg.PollFrequency,
		workgroup:      cfg.WorkGroup,
		resultMode:     cfg.ResultMode,
		config:         cfg.Config,
		timeout:        cfg.Timeout,
		catalog:        cfg.Catalog,
	}, nil
}

// Open is a more robust version of `db.Open`, as it accepts a raw aws.Config.
// This is useful if you have a complex AWS session since the driver doesn't
// currently attempt to serialize all options into a string.
func Open(cfg Config) (*sql.DB, error) {
	if cfg.Database == "" {
		return nil, ErrDatabaseRequired
	}

	// Check if AWS config is empty by checking Region field instead of comparing the whole struct
	if cfg.Config.Region == "" {
		return nil, ErrSessionRequired
	}

	if cfg.WorkGroup == "" {
		cfg.WorkGroup = "primary"
	}

	// This hack was copied from jackc/pgx. Sorry :(
	// https://github.com/jackc/pgx/blob/70a284f4f33a9cc28fd1223f6b83fb00deecfe33/stdlib/sql.go#L130-L136
	openFromSessionMutex.Lock()
	openFromSessionCount++
	name := fmt.Sprintf("athena-%d", openFromSessionCount)
	openFromSessionMutex.Unlock()

	sql.Register(name, &Driver{&cfg})
	return sql.Open(name, "")
}

// Config is the input to Open().
type Config struct {
	Config         aws.Config
	Database       string
	OutputLocation string
	WorkGroup      string

	PollFrequency time.Duration

	ResultMode ResultMode
	Timeout    uint
	Catalog    string
}

func configFromConnectionString(connStr string) (*Config, error) {
	args, err := url.ParseQuery(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	var cfg Config

	// Setup AWS config
	ctx := context.Background()
	region := args.Get("region")

	var optFns []func(*config.LoadOptions) error
	if region != "" {
		optFns = append(optFns, config.WithRegion(region))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS config: %w", err)
	}
	cfg.Config = awsCfg

	cfg.Database = args.Get("db")
	if cfg.Database == "" {
		return nil, ErrDatabaseRequired
	}

	cfg.OutputLocation = args.Get("output_location")
	cfg.WorkGroup = args.Get("workgroup")
	if cfg.WorkGroup == "" {
		cfg.WorkGroup = "primary"
	}

	frequencyStr := args.Get("poll_frequency")
	if frequencyStr != "" {
		cfg.PollFrequency, err = time.ParseDuration(frequencyStr)
		if err != nil {
			return nil, fmt.Errorf("invalid poll_frequency parameter: %w", err)
		}
	}

	cfg.ResultMode = ResultModeAPI
	modeValue := strings.ToLower(args.Get("result_mode"))
	switch {
	case modeValue == "dl" || modeValue == "download":
		cfg.ResultMode = ResultModeDL
	case modeValue == "gzip":
		cfg.ResultMode = ResultModeGzipDL
	case modeValue != "" && modeValue != "api":
		return nil, ErrInvalidResultMode
	}

	cfg.Timeout = timeOutLimitDefault
	if tm := args.Get("timeout"); tm != "" {
		if timeout, err := strconv.ParseUint(tm, 10, 32); err == nil {
			cfg.Timeout = uint(timeout)
		}
	}

	cfg.Catalog = CATALOG_AWS_DATA_CATALOG
	if ct := args.Get("catalog"); ct != "" {
		cfg.Catalog = ct
	}

	return &cfg, nil
}

// checkOutputLocation is to check if outputLocation should be obtained from workgroup.
func checkOutputLocation(resultMode ResultMode, outputLocation string) bool {
	return resultMode != ResultModeAPI && outputLocation == ""
}

// getOutputLocation is for getting output location value from workgroup when location value is empty.
func getOutputLocation(athenaClient *athena.Client, workGroup string) (string, error) {
	ctx := context.Background()
	var outputLocation string
	output, err := athenaClient.GetWorkGroup(
		ctx,
		&athena.GetWorkGroupInput{
			WorkGroup: aws.String(workGroup),
		},
	)
	if err == nil {
		outputLocation = *output.WorkGroup.Configuration.ResultConfiguration.OutputLocation
	}
	return outputLocation, err
}
