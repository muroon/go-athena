package athena

import (
	"database/sql/driver"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
	"io"
)

type rowsAPI struct {
	athena     athenaiface.AthenaAPI
	queryID    string
	resultMode ResultMode

	// use only api mode
	done          bool
	skipHeaderRow bool
	out           *athena.GetQueryResultsOutput
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

func (r *rowsAPI) fetchNextPage(token *string) (bool, error) {
	var err error
	r.out, err = r.athena.GetQueryResults(&athena.GetQueryResultsInput{
		QueryExecutionId: aws.String(r.queryID),
		NextToken:        token,
	})
	if err != nil {
		return false, err
	}

	var rowOffset = 0
	// First row of the first page contains header if the query is not DDL.
	// These are also available in *athena.Row.ResultSetMetadata.
	if r.skipHeaderRow {
		rowOffset = 1
		r.skipHeaderRow = false
	}

	if len(r.out.ResultSet.Rows) < rowOffset+1 {
		return false, nil
	}

	r.out.ResultSet.Rows = r.out.ResultSet.Rows[rowOffset:]
	return true, nil
}

func (r *rowsAPI) nextAPI(dest []driver.Value) error {
	// If nothing left to iterate...
	if len(r.out.ResultSet.Rows) == 0 {
		// And if nothing more to paginate...
		if r.out.NextToken == nil || *r.out.NextToken == "" {
			return io.EOF
		}

		cont, err := r.fetchNextPage(r.out.NextToken)
		if err != nil {
			return err
		}

		if !cont {
			return io.EOF
		}
	}

	// Shift to next row
	cur := r.out.ResultSet.Rows[0]
	columns := r.out.ResultSet.ResultSetMetadata.ColumnInfo
	if err := convertRow(columns, cur.Data, dest); err != nil {
		return err
	}

	r.out.ResultSet.Rows = r.out.ResultSet.Rows[1:]
	return nil
}

func (r *rowsAPI) Columns() []string {
	var columns []string
	for _, colInfo := range r.out.ResultSet.ResultSetMetadata.ColumnInfo {
		columns = append(columns, *colInfo.Name)
	}

	return columns
}

func (r *rowsAPI) ColumnTypeDatabaseTypeName(index int) string {
	colInfo := r.out.ResultSet.ResultSetMetadata.ColumnInfo[index]
	if colInfo.Type != nil {
		return *colInfo.Type
	}
	return ""
}

func (r *rowsAPI) Next(dest []driver.Value) error {
	return r.nextAPI(dest)
}

func (r *rowsAPI) Close() error {
	r.done = true
	return nil
}
