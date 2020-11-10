package athena

import (
	"context"
	"database/sql/driver"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
)

type rows struct {
	athena  athenaiface.AthenaAPI
	queryID string
	mode mode

	// use only api mode
	done          bool
	skipHeaderRow bool
	out           *athena.GetQueryResultsOutput

	// use only download mode
	downloadedRows *downloadedRows
}

type rowsConfig struct {
	Athena     athenaiface.AthenaAPI
	QueryID    string
	SkipHeader bool
	Mode       mode
	Session    *session.Session
	OutputLocation string
	Timeout uint
}

type downloadedRows struct {
	cursor int
	header []string
	data [][]string
}

func newRows(cfg rowsConfig) (*rows, error) {
	r := rows{
		athena:        cfg.Athena,
		queryID:       cfg.QueryID,
		skipHeaderRow: cfg.SkipHeader,
		mode:       cfg.Mode,
	}

	if r.isDownload() {
		ctx := context.Background()
		ctx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Timeout) * time.Second)
		defer cancel()

		err := make(chan error, 2)

		// download and set in memory
		go r.downloadCsvAsync(ctx, err, cfg.Session, cfg.OutputLocation)

		// get table metadata
		go r.getQueryResultsAsyncForCsv(ctx, err)

		for i := 0; i < 2; i++ {
			select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case e := <-err:
					if e != nil {
						return nil, e
					}
			}
		}

		return &r, nil
	}

	shouldContinue, err := r.fetchNextPage(nil)
	if err != nil {
		return nil, err
	}

	r.done = !shouldContinue
	return &r, nil
}

func (r *rows) Columns() []string {
	var columns []string

	if r.isDownload() {
		return r.downloadedRows.header
	}

	for _, colInfo := range r.out.ResultSet.ResultSetMetadata.ColumnInfo {
		columns = append(columns, *colInfo.Name)
	}

	return columns
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	colInfo := r.out.ResultSet.ResultSetMetadata.ColumnInfo[index]
	if colInfo.Type != nil {
		return *colInfo.Type
	}
	return ""
}

func (r *rows) Next(dest []driver.Value) error {
	if r.isDownload() {
		if r.downloadedRows.cursor >= len(r.downloadedRows.data) {
			return io.EOF
		}
		row := r.downloadedRows.data[r.downloadedRows.cursor]
		columns := r.out.ResultSet.ResultSetMetadata.ColumnInfo
		if err := convertRowFromCsv(columns, row, dest); err != nil {
			return err
		}

		r.downloadedRows.cursor++
		return nil
	}

	if r.done {
		return io.EOF
	}

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

func (r *rows) fetchNextPage(token *string) (bool, error) {
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

func (r *rows) Close() error {
	r.done = true
	return nil
}
