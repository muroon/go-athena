package athena

import (
	"context"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	uuid "github.com/satori/go.uuid"
)

// GetQueryResultsAPI defines the interface for Athena GetQueryResults operation
type GetQueryResultsAPI interface {
	GetQueryResults(ctx context.Context, params *athena.GetQueryResultsInput, optFns ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error)
}

// StartQueryExecutionAPI defines the interface for Athena StartQueryExecution operation
type StartQueryExecutionAPI interface {
	StartQueryExecution(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error)
}

// GetWorkGroupAPI defines the interface for Athena GetWorkGroup operation
type GetWorkGroupAPI interface {
	GetWorkGroup(ctx context.Context, params *athena.GetWorkGroupInput, optFns ...func(*athena.Options)) (*athena.GetWorkGroupOutput, error)
}

// StopQueryExecutionAPI defines the interface for Athena StopQueryExecution operation
type StopQueryExecutionAPI interface {
	StopQueryExecution(ctx context.Context, params *athena.StopQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StopQueryExecutionOutput, error)
}

// GetQueryExecutionAPI defines the interface for Athena GetQueryExecution operation
type GetQueryExecutionAPI interface {
	GetQueryExecution(ctx context.Context, params *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error)
}

type conn struct {
	athena         *athena.Client
	db             string
	OutputLocation string
	workgroup      string
	pollFrequency  time.Duration
	resultMode     ResultMode
	timeout        uint
	catalog        string
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		panic("Athena doesn't support prepared statements. Format your own arguments.")
	}

	rows, err := c.runQuery(ctx, query)
	return rows, err
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		panic("Athena doesn't support prepared statements. Format your own arguments.")
	}

	_, err := c.runQuery(ctx, query)
	return nil, err
}

func (c *conn) runQuery(ctx context.Context, query string) (driver.Rows, error) {
	queryID, err := c.startQuery(query)
	if err != nil {
		return nil, err
	}

	if err := c.waitOnQuery(ctx, queryID); err != nil {
		return nil, err
	}

	resultMode := c.resultMode
	if rmode, ok := getResultMode(ctx); ok {
		resultMode = rmode
	}

	var timeout uint = timeOutLimitDefault
	if tm, ok := getTimeout(ctx); ok {
		timeout = tm
	}

	var catalog string = c.catalog
	if ct, ok := getCatalog(ctx); ok {
		catalog = ct
	}

	cfg := rowsConfig{
		Athena:         c.athena,
		QueryID:        queryID,
		DB:             c.db,
		OutputLocation: c.OutputLocation,
		SkipHeader:     !isDDLQuery(query) && !isCTASQuery(query),
		ResultMode:     resultMode,
		Timeout:        timeout,
		Catalog:        catalog,
	}

	return newRows(cfg)
}

func (c *conn) dropCTASTable(ctx context.Context, table string) func() error {
	return func() error {
		queryID, err := c.startQuery(fmt.Sprintf("DROP TABLE %s", table))
		if err != nil {
			return err
		}
		return c.waitOnQuery(ctx, queryID)
	}
}

// startQuery starts an Athena query and returns its ID.
func (c *conn) startQuery(query string) (string, error) {
	input := &athena.StartQueryExecutionInput{
		QueryString: &query,
		QueryExecutionContext: &types.QueryExecutionContext{
			Database: &c.db,
			Catalog:  &c.catalog,
		},
		WorkGroup: &c.workgroup,
	}

	if c.OutputLocation != "" {
		input.ResultConfiguration = &types.ResultConfiguration{
			OutputLocation: &c.OutputLocation,
		}
	}

	resp, err := c.athena.StartQueryExecution(context.Background(), input)
	if err != nil {
		return "", err
	}

	return *resp.QueryExecutionId, nil
}

// waitOnQuery blocks until a query finishes, returning an error if it failed.
func (c *conn) waitOnQuery(ctx context.Context, queryID string) error {
	input := &athena.GetQueryExecutionInput{
		QueryExecutionId: &queryID,
	}

	var timeout uint = timeOutLimitDefault
	if tm, ok := getTimeout(ctx); ok {
		timeout = tm
	}

	start := time.Now()
	for {
		resp, err := c.athena.GetQueryExecution(ctx, input)
		if err != nil {
			return err
		}

		if resp.QueryExecution == nil {
			return fmt.Errorf("nil QueryExecution")
		}

		state := resp.QueryExecution.Status.State
		if state == types.QueryExecutionStateSucceeded {
			return nil
		}

		if state == types.QueryExecutionStateFailed ||
			state == types.QueryExecutionStateCancelled {
			return fmt.Errorf("query execution failed: %s", *resp.QueryExecution.Status.StateChangeReason)
		}

		if uint(time.Since(start).Seconds()) > timeout {
			// timeout
			c.stopQuery(queryID)
			return fmt.Errorf("query timeout after %d seconds", timeout)
		}

		time.Sleep(c.pollFrequency)
	}
}

func (c *conn) stopQuery(queryID string) error {
	input := &athena.StopQueryExecutionInput{
		QueryExecutionId: &queryID,
	}

	_, err := c.athena.StopQueryExecution(context.Background(), input)
	return err
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.prepareContext(context.Background(), query)
}

func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stmt, err := c.prepareContext(ctx, query)

	select {
	default:
	case <-ctx.Done():
		stmt.Close()
		return nil, ctx.Err()
	}

	return stmt, err
}

func (c *conn) prepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	// resultMode
	isSelect := isSelectQuery(query)
	resultMode := c.resultMode
	if rmode, ok := getResultMode(ctx); ok {
		resultMode = rmode
	}
	if !isSelect {
		resultMode = ResultModeAPI
	}

	// ctas
	var ctasTable string
	var afterDownload func() error
	if isCreatingCTASTable(isSelect, resultMode) {
		// Create AS Select
		ctasTable = fmt.Sprintf("tmp_ctas_%v", strings.Replace(uuid.NewV4().String(), "-", "", -1))
		query = fmt.Sprintf("CREATE TABLE %s WITH (format='TEXTFILE') AS %s", ctasTable, query)
		afterDownload = c.dropCTASTable(ctx, ctasTable)
	}

	numInput := len(strings.Split(query, "?")) - 1

	// prepare
	prepareKey := fmt.Sprintf("tmp_prepare_%v", strings.Replace(uuid.NewV4().String(), "-", "", -1))
	newQuery := fmt.Sprintf("PREPARE %s FROM %s", prepareKey, query)

	queryID, err := c.startQuery(newQuery)
	if err != nil {
		return nil, err
	}

	if err := c.waitOnQuery(ctx, queryID); err != nil {
		return nil, err
	}

	return &stmtAthena{
		prepareKey:    prepareKey,
		numInput:      numInput,
		ctasTable:     ctasTable,
		afterDownload: afterDownload,
		conn:          c,
		resultMode:    resultMode,
	}, nil
}

func (c *conn) Begin() (driver.Tx, error) {
	panic("Athena doesn't support transactions")
}

func (c *conn) Close() error {
	return nil
}

var _ driver.QueryerContext = (*conn)(nil)
var _ driver.ExecerContext = (*conn)(nil)

// HACK(tejasmanohar): database/sql calls Prepare() if your driver doesn't implement
// Queryer. Regardless, db.Query/Exec* calls Query/Exec-Context so I've filed a bug--
// https://github.com/golang/go/issues/22980.
func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	panic("Query() is noop")
}

func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	panic("Exec() is noop")
}

var _ driver.Queryer = (*conn)(nil)
var _ driver.Execer = (*conn)(nil)

// supported DDL statements by Athena
// https://docs.aws.amazon.com/athena/latest/ug/language-reference.html
var ddlQueryRegex = regexp.MustCompile(`(?i)^(ALTER|CREATE|DESCRIBE|DROP|MSCK|SHOW)`)

func isDDLQuery(query string) bool {
	return ddlQueryRegex.Match([]byte(query))
}

func isSelectQuery(query string) bool {
	return regexp.MustCompile(`(?i)^SELECT`).Match([]byte(query))
}

func isCTASQuery(query string) bool {
	return regexp.MustCompile(`(?i)^CREATE.+AS\s+SELECT`).Match([]byte(query))
}

func isCreatingCTASTable(isSelect bool, resultMode ResultMode) bool {
	return isSelect && resultMode == ResultModeGzipDL
}
