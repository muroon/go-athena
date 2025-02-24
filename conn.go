package athena

import (
	"context"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		panic("Athena doesn't support prepared statements. Format your own arguments.")
	}

	_, err := c.runQuery(ctx, query)
	return nil, err
}

func (c *conn) runQuery(ctx context.Context, query string) (driver.Rows, error) {
	// result mode
	isSelect := isSelectQuery(query)
	resultMode := c.resultMode
	if rmode, ok := getResultMode(ctx); ok {
		resultMode = rmode
	}
	if !isSelect {
		resultMode = ResultModeAPI
	}

	// timeout
	timeout := c.timeout
	if to, ok := getTimeout(ctx); ok {
		timeout = to
	}

	// catalog
	catalog := c.catalog
	if cat, ok := getCatalog(ctx); ok {
		catalog = cat
	}

	// output location (with empty value)
	if checkOutputLocation(resultMode, c.outputLocation) {
		var err error
		c.outputLocation, err = getOutputLocation(c.athena, c.workgroup)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get output location")
		}
	}

	// mode ctas
	var ctasTable string
	var afterDownload func() error
	if isCreatingCTASTable(isSelect, resultMode) {
		// Create AS Select
		ctasTable = fmt.Sprintf("tmp_ctas_%v", strings.Replace(uuid.NewV4().String(), "-", "", -1))
		query = fmt.Sprintf("CREATE TABLE %s WITH (format='TEXTFILE') AS %s", ctasTable, query)
		afterDownload = c.dropCTASTable(ctx, ctasTable)
	}

	queryID, err := c.startQuery(query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to start query")
	}

	if err := c.waitOnQuery(ctx, queryID); err != nil {
		return nil, errors.Wrap(err, "failed to wait for query completion")
	}

	return newRows(rowsConfig{
		Athena:         c.athena,
		QueryID:        queryID,
		SkipHeader:     !isDDLQuery(query),
		ResultMode:     resultMode,
		OutputLocation: c.outputLocation,
		Timeout:        timeout,
		AfterDownload:  afterDownload,
		CTASTable:      ctasTable,
		DB:             c.database,
		Catalog:        catalog,
	})
}

func (c *conn) dropCTASTable(ctx context.Context, table string) func() error {
	return func() error {
		query := fmt.Sprintf("DROP TABLE %s", table)

		queryID, err := c.startQuery(query)
		if err != nil {
			return errors.Wrapf(err, "failed to start drop table query for %s", table)
		}

		return errors.Wrapf(c.waitOnQuery(ctx, queryID), "failed to wait for drop table completion for %s", table)
	}
}

// startQuery starts an Athena query and returns its ID.
func (c *conn) startQuery(query string) (string, error) {
	resp, err := c.athena.StartQueryExecution(context.TODO(), &athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		QueryExecutionContext: &types.QueryExecutionContext{
			Database: aws.String(c.database),
		},
		ResultConfiguration: &types.ResultConfiguration{
			OutputLocation: aws.String(c.outputLocation),
		},
		WorkGroup: aws.String(c.workgroup),
	})
	if err != nil {
		return "", errors.Wrap(err, "failed to start query execution")
	}

	return *resp.QueryExecutionId, nil
}

// waitOnQuery blocks until a query finishes, returning an error if it failed.
func (c *conn) waitOnQuery(ctx context.Context, queryID string) error {
	for {
		statusResp, err := c.athena.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryID),
		})
		if err != nil {
			return errors.Wrap(err, "failed to get query execution status")
		}

		switch statusResp.QueryExecution.Status.State {
		case types.QueryExecutionStateCancelled:
			return errors.New("query execution was cancelled")
		case types.QueryExecutionStateFailed:
			reason := *statusResp.QueryExecution.Status.StateChangeReason
			return errors.Errorf("query execution failed: %s", reason)
		case types.QueryExecutionStateSucceeded:
			return nil
		case types.QueryExecutionStateQueued:
		case types.QueryExecutionStateRunning:
		}

		select {
		case <-ctx.Done():
			_, err := c.athena.StopQueryExecution(context.TODO(), &athena.StopQueryExecutionInput{
				QueryExecutionId: aws.String(queryID),
			})
			if err != nil {
				return errors.Wrap(err, "failed to stop query execution after context cancellation")
			}
			return errors.Wrap(ctx.Err(), "context cancelled while waiting for query")
		case <-time.After(c.pollFrequency):
			continue
		}
	}
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
		return nil, errors.Wrap(err, "failed to start prepare statement query")
	}

	if err := c.waitOnQuery(ctx, queryID); err != nil {
		return nil, errors.Wrap(err, "failed to wait for prepare statement completion")
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
