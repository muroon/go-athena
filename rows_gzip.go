package athena

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

type rowsGzipDL struct {
	athena     *athena.Client
	queryID    string
	resultMode ResultMode
	ctasTable  string
	db         string
	catalog    string

	columnNames []string
	columnTypes []*ColumnType
	currentData []types.Row
	done        bool
}

func newRowsGzipDL(cfg rowsConfig) (*rowsGzipDL, error) {
	client, ok := cfg.Athena.(*athena.Client)
	if !ok {
		return nil, fmt.Errorf("invalid athena client type")
	}
	r := &rowsGzipDL{
		athena:     client,
		queryID:    cfg.QueryID,
		resultMode: cfg.ResultMode,
		ctasTable:  cfg.CTASTable,
		db:         cfg.DB,
		catalog:    cfg.Catalog,
	}
	err := r.init(cfg)
	return r, err
}

func (r *rowsGzipDL) init(cfg rowsConfig) error {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Timeout)*time.Second)
	defer cancel()

	// get table metadata
	columns, err := r.getColumnInfo(ctx)
	if err != nil {
		return err
	}

	r.columnNames = make([]string, len(columns))
	r.columnTypes = make([]*ColumnType, len(columns))
	for i, col := range columns {
		r.columnNames[i] = *col.Name
		r.columnTypes[i] = NewColumnType(*col.Type)
	}

	return nil
}

func (r *rowsGzipDL) Columns() []string {
	return r.columnNames
}

func (r *rowsGzipDL) Close() error {
	return nil
}

func (r *rowsGzipDL) Next(dest []driver.Value) error {
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

func (r *rowsGzipDL) ColumnTypeDatabaseTypeName(index int) string {
	return r.columnTypes[index].DatabaseTypeName()
}

func (r *rowsGzipDL) fetchNextPage(nextToken *string) (bool, error) {
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

	if resp.ResultSet == nil || len(resp.ResultSet.Rows) == 0 {
		return false, nil
	}

	r.currentData = resp.ResultSet.Rows[1:] // Skip header row
	return resp.NextToken != nil, nil
}

func (r *rowsGzipDL) getColumnInfo(ctx context.Context) ([]types.ColumnInfo, error) {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 0", r.ctasTable)

	input := &athena.StartQueryExecutionInput{
		QueryString: &query,
		QueryExecutionContext: &types.QueryExecutionContext{
			Database: &r.db,
			Catalog:  &r.catalog,
		},
	}

	resp, err := r.athena.StartQueryExecution(ctx, input)
	if err != nil {
		return nil, err
	}

	queryID := *resp.QueryExecutionId
	getResultsInput := &athena.GetQueryResultsInput{
		QueryExecutionId: &queryID,
	}

	results, err := r.athena.GetQueryResults(ctx, getResultsInput)
	if err != nil {
		return nil, err
	}

	if results.ResultSet == nil || results.ResultSet.ResultSetMetadata == nil {
		return nil, fmt.Errorf("invalid response format")
	}

	return results.ResultSet.ResultSetMetadata.ColumnInfo, nil
}
