package athena

import (
	"bufio"
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type rowsDL struct {
	athena         AthenaAPI
	queryID        string
	resultMode     ResultMode
	out            *athena.GetQueryResultsOutput
	downloadedRows *downloadedRows
}

func newRowsDL(cfg rowsConfig) (*rowsDL, error) {
	r := &rowsDL{
		athena:     cfg.Athena,
		queryID:    cfg.QueryID,
		resultMode: cfg.ResultMode,
	}
	err := r.init(cfg)
	return r, err
}

func (r *rowsDL) init(cfg rowsConfig) error {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Timeout)*time.Second)
	defer cancel()

	err := make(chan error, 2)

	// download and set in memory
	go r.downloadCsvAsync(ctx, err, cfg.Config, cfg.OutputLocation)

	// get table metadata
	go r.getQueryResultsAsyncForCsv(ctx, err)

	for i := 0; i < 2; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-err:
			if e != nil {
				return e
			}
		}
	}
	return nil
}

func (r *rowsDL) downloadCsvAsync(
	ctx context.Context,
	errCh chan error,
	cfg aws.Config,
	location string,
) {
	errCh <- r.downloadCsv(ctx, cfg, location)
}

func (r *rowsDL) downloadCsv(ctx context.Context, cfg aws.Config, location string) error {
	// remove the first 5 characters "s3://" from location
	bucketName := location[5:]
	slash := strings.Index(bucketName, "/")
	if slash == -1 {
		return fmt.Errorf("invalid S3 location format: %s", location)
	}
	bucket := bucketName[:slash]
	prefix := bucketName[slash+1:]
	objectKey := fmt.Sprintf("%s%s.csv", prefix, r.queryID)

	// Create an S3 client
	s3Client := s3.NewFromConfig(cfg)
	resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return err
	}

	// Read the object content
	data, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}

	fields, err := getRecordsForDL(strings.NewReader(string(data)))
	if err != nil {
		return err
	}
	r.downloadedRows = &downloadedRows{
		field: fields[1:],
	}
	return nil
}

func (r *rowsDL) getQueryResultsAsyncForCsv(ctx context.Context, errCh chan error) {
	var err error
	r.out, err = r.athena.GetQueryResults(ctx, &athena.GetQueryResultsInput{
		QueryExecutionId: aws.String(r.queryID),
		MaxResults:       aws.Int32(1),
	})
	errCh <- err
}

func (r *rowsDL) nextDownload(dest []driver.Value) error {
	if r.downloadedRows.cursor >= len(r.downloadedRows.field) {
		return io.EOF
	}
	row := r.downloadedRows.field[r.downloadedRows.cursor]
	columns := r.out.ResultSet.ResultSetMetadata.ColumnInfo
	if err := convertRowFromCsv(columns, row, dest); err != nil {
		return err
	}

	r.downloadedRows.cursor++
	return nil
}

func (r *rowsDL) Columns() []string {
	var columns []string
	for _, colInfo := range r.out.ResultSet.ResultSetMetadata.ColumnInfo {
		columns = append(columns, *colInfo.Name)
	}

	return columns
}

func (r *rowsDL) ColumnTypeDatabaseTypeName(index int) string {
	colInfo := r.out.ResultSet.ResultSetMetadata.ColumnInfo[index]
	if colInfo.Type != nil {
		return *colInfo.Type
	}
	return ""
}

func (r *rowsDL) Next(dest []driver.Value) error {
	return r.nextDownload(dest)
}

func (r *rowsDL) Close() error {
	return nil
}

func getRecordsForDL(reader io.Reader) ([][]downloadField, error) {
	records := make([][]downloadField, 0)

	scanner := bufio.NewScanner(reader)

	// read line by line
	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, err
		}
		b := scanner.Bytes()
		useDoubleQuote := false
		delimiter := false
		field := ""
		record := make([]downloadField, 0)
		for {
			r, width := utf8.DecodeRune(b)
			if len(field) == 0 {
				useDoubleQuote = r == '"'
			}

			if r == ',' {
				delimiter = true
				if useDoubleQuote {
					delimiter = false
					if len(field) > 0 && field[len(field)-1:] == string('"') {
						field = field[1 : len(field)-1]
						delimiter = true
					}
				}
			}

			if delimiter {
				isNil := !useDoubleQuote && len(field) == 0
				row := downloadField{
					isNil: isNil,
					val:   field,
				}
				record = append(record, row)
				field = ""
				delimiter = false
			} else {
				field += string(r)
			}
			if width >= len(b) {
				if useDoubleQuote {
					if len(field) > 0 && field[len(field)-1:] == string('"') {
						field = field[1 : len(field)-1]
					}
				}
				isNil := !useDoubleQuote && len(field) == 0
				row := downloadField{
					isNil: isNil,
					val:   field,
				}
				record = append(record, row)
				break
			}
			b = b[width:]
		}

		records = append(records, record)
	}

	return records, nil
}
