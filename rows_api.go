package athena

import (
	"context"
	"database/sql/driver"
	"io"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

type rowsAPI struct {
	athena        GetQueryResultsAPI
	queryID       string
	skipHeaderRow bool
	resultMode    ResultMode

	currentData []types.Row
	done        bool
	columnNames []string
	columnTypes []*columnType
}

func newRowsAPI(cfg rowsConfig) (*rowsAPI, error) {
	r := &rowsAPI{
		athena:        cfg.Athena,
		queryID:       cfg.QueryID,
		skipHeaderRow: cfg.SkipHeader,
		resultMode:    cfg.ResultMode,
	}
	err := r.init(cfg)
	return r, err
}

func (r *rowsAPI) init(cfg rowsConfig) error {
	shouldContinue, err := r.fetchNextPage(nil)
	if err != nil {
		return err
	}

	r.done = !shouldContinue
	return nil
}

func (r *rowsAPI) Columns() []string {
	return r.columnNames
}

func (r *rowsAPI) Close() error {
	return nil
}

func (r *rowsAPI) Next(dest []driver.Value) error {
	if len(r.currentData) == 0 {
		if r.done {
			return io.EOF
		}

		shouldContinue, err := r.fetchNextPage(nil)
		if err != nil {
			return err
		}
		if !shouldContinue {
			return io.EOF
		}
	}

	currentRow := r.currentData[0]
	r.currentData = r.currentData[1:]

	for i, val := range currentRow.Data {
		if val.VarCharValue == nil {
			dest[i] = nil
			continue
		}

		v, err := r.columnTypes[i].ConvertValue(*val.VarCharValue)
		if err != nil {
			return err
		}
		dest[i] = v
	}

	return nil
}

func (r *rowsAPI) fetchNextPage(nextToken *string) (bool, error) {
	input := &athena.GetQueryResultsInput{
		QueryExecutionId: &r.queryID,
	}
	if nextToken != nil {
		input.NextToken = nextToken
	}

	resp, err := r.athena.GetQueryResults(context.Background(), input)
	if err != nil {
		return false, err
	}

	if resp.ResultSet == nil {
		return false, nil
	}

	// Initialize column names and types if this is our first fetch
	if r.columnNames == nil {
		r.columnNames = make([]string, len(resp.ResultSet.ResultSetMetadata.ColumnInfo))
		r.columnTypes = make([]*columnType, len(resp.ResultSet.ResultSetMetadata.ColumnInfo))
		for i, info := range resp.ResultSet.ResultSetMetadata.ColumnInfo {
			r.columnNames[i] = *info.Name
			r.columnTypes[i] = newColumnType(*info.Type)
		}
	}

	rows := resp.ResultSet.Rows
	if len(rows) == 0 {
		return false, nil
	}

	// Skip the header row if needed and it exists
	if r.skipHeaderRow && len(rows) > 0 {
		rows = rows[1:]
	}
	r.skipHeaderRow = false

	r.currentData = rows
	return resp.NextToken != nil, nil
}

func (r *rowsAPI) ColumnTypeDatabaseTypeName(index int) string {
	return r.columnTypes[index].DatabaseTypeName()
}
