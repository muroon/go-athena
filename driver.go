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

var (
	openFromSessionMutex sync.Mutex
	openFromSessionCount int
)

const (
	// timeOutLimitDefault athena's timeout limit
	timeOutLimitDefault uint = 1800
	// CATALOG_AWS_DATA_CATALOG default catalog name
	CATALOG_AWS_DATA_CATALOG = "AwsDataCatalog"
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

	// Create Athena client with AWS config
	athenaClient := athena.NewFromConfig(cfg.AWSConfig)

	// output location (with empty value)
	if checkOutputLocation(cfg.ResultMode, cfg.OutputLocation) {
		var err error
		cfg.OutputLocation, err = getOutputLocation(athenaClient, cfg.WorkGroup)
		if err != nil {
			return nil, err
		}
	}

	return &conn{
		config:         cfg.AWSConfig,
		athena:         athenaClient,
		db:             cfg.Database,
		OutputLocation: cfg.OutputLocation,
		pollFrequency:  cfg.PollFrequency,
		workgroup:      cfg.WorkGroup,
		resultMode:     cfg.ResultMode,
		timeout:        cfg.Timeout,
		catalog:        cfg.Catalog,
	}, nil
}

// Open is a more robust version of `db.Open`, as it accepts a raw aws.Config.
func Open(cfg Config) (*sql.DB, error) {
	if cfg.Database == "" {
		return nil, errors.New("db is required")
	}
	if cfg.WorkGroup == "" {
		cfg.WorkGroup = "primary"
	}

	openFromSessionMutex.Lock()
	openFromSessionCount++
	name := fmt.Sprintf("athena-%d", openFromSessionCount)
	openFromSessionMutex.Unlock()

	sql.Register(name, &Driver{&cfg})
	return sql.Open(name, "")
}

// Config is the input to Open().
type Config struct {
	AWSConfig      aws.Config
	Database       string
	OutputLocation string
	WorkGroup      string
	PollFrequency  time.Duration
	ResultMode     ResultMode
	Timeout        uint
	Catalog        string
}

func configFromConnectionString(connStr string) (*Config, error) {
	args, err := url.ParseQuery(connStr)
	if err != nil {
		return nil, err
	}

	var cfg Config
	ctx := context.Background()
	cfgOpts := []func(*config.LoadOptions) error{}

	if region := args.Get("region"); region != "" {
		cfgOpts = append(cfgOpts, config.WithRegion(region))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, cfgOpts...)
	if err != nil {
		return nil, err
	}

	cfg.AWSConfig = awsCfg
	cfg.Database = args.Get("db")
	cfg.OutputLocation = args.Get("output_location")
	cfg.WorkGroup = args.Get("workgroup")
	if cfg.WorkGroup == "" {
		cfg.WorkGroup = "primary"
	}

	frequencyStr := args.Get("poll_frequency")
	if frequencyStr != "" {
		cfg.PollFrequency, err = time.ParseDuration(frequencyStr)
		if err != nil {
			return nil, fmt.Errorf("invalid poll_frequency parameter: %s", frequencyStr)
		}
	}

	cfg.ResultMode = ResultModeAPI
	modeValue := strings.ToLower(args.Get("result_mode"))
	switch {
	case modeValue == "dl" || modeValue == "download":
		cfg.ResultMode = ResultModeDL
	case modeValue == "gzip":
		cfg.ResultMode = ResultModeGzipDL
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
	return resultMode != ResultModeGzipDL && outputLocation == ""
}

// getOutputLocation is for getting output location value from workgroup when location value is empty.
func getOutputLocation(athenaClient *athena.Client, workGroup string) (string, error) {
	input := &athena.GetWorkGroupInput{
		WorkGroup: &workGroup,
	}

	resp, err := athenaClient.GetWorkGroup(context.Background(), input)
	if err != nil {
		return "", err
	}

	if resp.WorkGroup == nil || resp.WorkGroup.Configuration == nil || resp.WorkGroup.Configuration.ResultConfiguration == nil {
		return "", fmt.Errorf("workgroup %s has no output location configured", workGroup)
	}

	outputLocation := resp.WorkGroup.Configuration.ResultConfiguration.OutputLocation
	if outputLocation == nil || *outputLocation == "" {
		return "", fmt.Errorf("workgroup %s has no output location configured", workGroup)
	}

	return *outputLocation, nil
}
