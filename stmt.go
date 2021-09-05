package athena

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/prestodb/presto-go-client/presto"
	"strconv"
	"strings"
)

type stmtAthena struct {
	prepareKey    string
	numInput      int
	ctasTable     string
	afterDownload func() error
	conn          *conn
	resultMode    ResultMode
}

func (s *stmtAthena) Close() error {
	query := fmt.Sprintf("DEALLOCATE PREPARE %s", s.prepareKey)
	_, err := s.conn.startQuery(query)
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

	ctx := context.Background()

	query, err := s.makeQuery(ctx, values)
	if err != nil {
		return nil, err
	}
	_, err = s.runQuery(ctx, query)
	return nil, err
}

func (s *stmtAthena) Query(args []driver.Value) (driver.Rows, error) {
	values := make([]interface{}, 0, len(args))
	for _, val := range args {
		values = append(values, val)
	}

	ctx := context.Background()

	query, err := s.makeQuery(ctx, values)
	if err != nil {
		return nil, err
	}
	return s.runQuery(ctx, query)
}

func (s *stmtAthena) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	values := make([]interface{}, 0, len(args))
	for _, val := range args {
		values = append(values, val.Value)
	}

	query, err := s.makeQuery(ctx, values)
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

	query, err := s.makeQuery(ctx, values)
	if err != nil {
		return nil, err
	}
	return s.runQuery(ctx, query)
}

func (s *stmtAthena) makeQuery(ctx context.Context, args []interface{}) (string, error) {
	params := make([]string, 0, len(args))
	for _, arg := range args {
		var param string
		param, err := serial(ctx, arg)
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
	// timeout
	timeout := s.conn.timeout
	if to, ok := getTimeout(ctx); ok {
		timeout = to
	}

	// catalog
	catalog := s.conn.catalog
	if cat, ok := getCatalog(ctx); ok {
		catalog = cat
	}

	// output location (with empty value)
	if checkOutputLocation(s.resultMode, s.conn.OutputLocation) {
		var err error
		s.conn.OutputLocation, err = getOutputLocation(s.conn.athena, s.conn.workgroup)
		if err != nil {
			return nil, err
		}
	}

	queryID, err := s.conn.startQuery(query)
	if err != nil {
		return nil, err
	}

	if err := s.conn.waitOnQuery(ctx, queryID); err != nil {
		return nil, err
	}

	return newRows(rowsConfig{
		Athena:         s.conn.athena,
		QueryID:        queryID,
		SkipHeader:     !isDDLQuery(query),
		ResultMode:     s.resultMode,
		Session:        s.conn.session,
		OutputLocation: s.conn.OutputLocation,
		Timeout:        timeout,
		AfterDownload:  s.afterDownload,
		CTASTable:      s.ctasTable,
		DB:             s.conn.db,
		Catalog:        catalog,
	})
}

func serial(ctx context.Context, v interface{}) (string, error) {
	forceNumericString := getForNumericString(ctx)
	if !forceNumericString {
		if x, ok := v.(string); ok {
			if _, err := strconv.ParseFloat(string(x), 64); err == nil {
				return presto.Serial(presto.Numeric(x))
			}
		}

		if x, ok := v.([]string); ok {
			isNumeric := true
		loop:
			for _, v := range x {
				if _, err := strconv.ParseFloat(v, 64); err != nil {
					isNumeric = false
					break loop
				}
			}

			if isNumeric {
				l := make([]presto.Numeric, 0, len(x))
				for _, v := range x {
					l = append(l, presto.Numeric(v))
				}
				return presto.Serial(l)
			}
		}
	}

	return presto.Serial(v)
}
