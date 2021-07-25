package athena

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
	"github.com/prestodb/presto-go-client/presto"
	"strconv"
	"strings"
	"time"
)

type stmtAthena struct {
	prepareKey    string
	numInput      int
	ctasTable     string
	afterDownload func() error

	athena         athenaiface.AthenaAPI
	db             string
	OutputLocation string
	workgroup      string

	pollFrequency time.Duration

	resultMode ResultMode
	session    *session.Session
	timeout    uint
	catalog    string
}

func (s *stmtAthena) Close() error {
	query := fmt.Sprintf("DEALLOCATE PREPARE %s", s.prepareKey)
	_, err := s.startQuery(query)
	return err
}

func (s *stmtAthena) NumInput() int {
	return s.numInput
}

func (s *stmtAthena) Exec(args []driver.Value) (driver.Result, error) {
	values := make([]interface{}, 0, len(args))
	for _, val := range args {
		values = append(values, val)
	}

	query, err := s.makeQuery(values)
	if err != nil {
		return nil, err
	}
	_, err = s.runQuery(context.Background(), query)
	return nil, err
}

func (s *stmtAthena) Query(args []driver.Value) (driver.Rows, error) {
	values := make([]interface{}, 0, len(args))
	for _, val := range args {
		values = append(values, val)
	}

	query, err := s.makeQuery(values)
	if err != nil {
		return nil, err
	}
	return s.runQuery(context.Background(), query)
}

func (s *stmtAthena) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	values := make([]interface{}, 0, len(args))
	for _, val := range args {
		values = append(values, val.Value)
	}

	query, err := s.makeQuery(values)
	if err != nil {
		return nil, err
	}
	_, err = s.runQuery(ctx, query)
	return nil, err
}

func (s *stmtAthena) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	values := make([]interface{}, 0, len(args))
	for _, val := range args {
		values = append(values, val.Value)
	}

	query, err := s.makeQuery(values)
	if err != nil {
		return nil, err
	}
	return s.runQuery(ctx, query)
}

func (s *stmtAthena) makeQuery(args []interface{}) (string, error) {
	params := make([]string, 0, len(args))
	for _, arg := range args {
		var param string
		param, err := serial(arg)
		if err != nil {
			return "", err
		}

		params = append(params, param)
	}

	var query string
	if len(params) > 0 {
		query = fmt.Sprintf("EXECUTE %s USING %s", s.prepareKey, strings.Join(params, ","))
	} else {
		query = fmt.Sprintf("EXECUTE %s", s.prepareKey)
	}
	return query, nil
}

func (s *stmtAthena) runQuery(ctx context.Context, query string) (driver.Rows, error) {
	/*
		// result mode
		isSelect := isSelectQuery(query)
		resultMode := s.resultMode
		if rmode, ok := getResultMode(ctx); ok {
			resultMode = rmode
		}
		if !isSelect {
			resultMode = ResultModeAPI
		}
	*/

	// timeout
	timeout := s.timeout
	if to, ok := getTimeout(ctx); ok {
		timeout = to
	}

	// catalog
	catalog := s.catalog
	if cat, ok := getCatalog(ctx); ok {
		catalog = cat
	}

	// output location (with empty value)
	if checkOutputLocation(s.resultMode, s.OutputLocation) {
		var err error
		s.OutputLocation, err = getOutputLocation(s.athena, s.workgroup)
		if err != nil {
			return nil, err
		}
	}

	// mode ctas
	//var ctasTable string
	//var afterDownload func() error
	/*
		if isSelect && resultMode == ResultModeGzipDL {
			// Create AS Select
			ctasTable = fmt.Sprintf("tmp_ctas_%v", strings.Replace(uuid.NewV4().String(), "-", "", -1))
			query = fmt.Sprintf("CREATE TABLE %s WITH (format='TEXTFILE') AS %s", ctasTable, query)
			afterDownload = c.dropCTASTable(ctx, ctasTable)
		}
	*/

	queryID, err := s.startQuery(query)
	if err != nil {
		return nil, err
	}

	if err := s.waitOnQuery(ctx, queryID); err != nil {
		return nil, err
	}

	return newRows(rowsConfig{
		Athena:         s.athena,
		QueryID:        queryID,
		SkipHeader:     !isDDLQuery(query),
		ResultMode:     s.resultMode,
		Session:        s.session,
		OutputLocation: s.OutputLocation,
		Timeout:        timeout,
		AfterDownload:  s.afterDownload,
		CTASTable:      s.ctasTable,
		DB:             s.db,
		Catalog:        catalog,
	})
}

// startQuery starts an Athena query and returns its ID.
func (s *stmtAthena) startQuery(query string) (string, error) {
	resp, err := s.athena.StartQueryExecution(&athena.StartQueryExecutionInput{
		QueryString: aws.String(query),
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: aws.String(s.db),
		},
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String(s.OutputLocation),
		},
		WorkGroup: aws.String(s.workgroup),
	})
	if err != nil {
		return "", err
	}

	return *resp.QueryExecutionId, nil
}

// waitOnQuery blocks until a query finishes, returning an error if it failed.
func (s *stmtAthena) waitOnQuery(ctx context.Context, queryID string) error {
	for {
		statusResp, err := s.athena.GetQueryExecutionWithContext(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryID),
		})
		if err != nil {
			return err
		}

		switch *statusResp.QueryExecution.Status.State {
		case athena.QueryExecutionStateCancelled:
			return context.Canceled
		case athena.QueryExecutionStateFailed:
			reason := *statusResp.QueryExecution.Status.StateChangeReason
			return errors.New(reason)
		case athena.QueryExecutionStateSucceeded:
			return nil
		case athena.QueryExecutionStateQueued:
		case athena.QueryExecutionStateRunning:
		}

		select {
		case <-ctx.Done():
			s.athena.StopQueryExecution(&athena.StopQueryExecutionInput{
				QueryExecutionId: aws.String(queryID),
			})

			return ctx.Err()
		case <-time.After(s.pollFrequency):
			continue
		}
	}
}

func serial(v interface{}) (string, error) {
	switch x := v.(type) {
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), nil

	default:
		return presto.Serial(x)
	}
}
