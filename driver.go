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
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

const (
	timeOutLimitDefault             = 300
	CATALOG_AWS_DATA_CATALOG string = "AwsDataCatalog"
)

// Driver implements database/sql/driver.Driver
type Driver struct{}

// Open returns a new connection to the database.
func (d *Driver) Open(name string) (driver.Conn, error) {
	connector, err := Open(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

type connector struct {
	conn *conn
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	return c.conn, nil
}

func (c *connector) Driver() driver.Driver {
	return &Driver{}
}

func Open(name string) (driver.Connector, error) {
	cfg, err := configFromConnectionString(name)
	if err != nil {
		return nil, err
	}

	athenaClient := athena.NewFromConfig(*cfg.AWSConfig)

	if checkOutputLocation(cfg.ResultMode, cfg.OutputLocation) {
		outputLocation, err := getOutputLocation(athenaClient, cfg.WorkGroup)
		if err != nil {
			return nil, err
		}
		cfg.OutputLocation = outputLocation
	}

	c := &conn{
		athena:         athenaClient,
		database:       cfg.Database,
		outputLocation: cfg.OutputLocation,
		workgroup:      cfg.WorkGroup,
		resultMode:     cfg.ResultMode,
		pollFrequency:  cfg.PollFrequency,
		timeout:        cfg.Timeout,
		catalog:        cfg.Catalog,
	}

	return &connector{conn: c}, nil
}

// Config is the input to Open().
type Config struct {
	AWSConfig      *aws.Config
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

	awsCfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, err
	}
	if region := args.Get("region"); region != "" {
		awsCfg.Region = region
	}
	cfg.AWSConfig = &awsCfg

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

func init() {
	sql.Register("athena", &Driver{})
}

type conn struct {
	athena         *athena.Client
	database       string
	outputLocation string
	workgroup      string
	resultMode     ResultMode
	pollFrequency  time.Duration
	timeout        uint
	catalog        string
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, errors.New("arguments are not supported")
	}

	input := &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		QueryExecutionContext: &types.QueryExecutionContext{
			Database: aws.String(c.database),
			Catalog:  aws.String(c.catalog),
		},
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: aws.String(c.outputLocation),
		},
		WorkGroup: aws.String(c.workgroup),
	}

	output, err := c.athena.StartQueryExecution(ctx, input)
	if err != nil {
		return nil, err
	}

	queryID := aws.ToString(output.QueryExecutionId)

	return newRows(rowsConfig{
		Athena:         c.athena,
		QueryID:        queryID,
		SkipHeader:     true,
		ResultMode:     c.resultMode,
		OutputLocation: c.outputLocation,
		Timeout:        c.timeout,
	})
}

func checkOutputLocation(resultMode ResultMode, outputLocation string) bool {
	return resultMode != ResultModeAPI && outputLocation == ""
}

func getOutputLocation(athenaClient *athena.Client, workGroup string) (string, error) {
	var outputLocation string
	output, err := athenaClient.GetWorkGroup(context.Background(),
		&athena.GetWorkGroupInput{
			WorkGroup: aws.String(workGroup),
		},
	)
	if err == nil {
		outputLocation = aws.ToString(output.WorkGroup.Configuration.ResultConfiguration.OutputLocation)
	}
	return outputLocation, err
}
