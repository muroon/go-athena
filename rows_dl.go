package athena

import (
	"bufio"
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type columnType struct {
	typeName string
}

func newColumnType(typeName string) *columnType {
	return &columnType{typeName: typeName}
}

func (ct *columnType) DatabaseTypeName() string {
	return ct.typeName
}

func (ct *columnType) ConvertValue(val string) (interface{}, error) {
	return convertValue(ct.typeName, &val)
}

type rowsDL struct {
	athena     *athena.Client
	queryID    string
	resultMode ResultMode

	columnNames []string
	columnTypes []*columnType
	records     [][]downloadField
	recordPtr   int
}

func newRowsDL(cfg rowsConfig) (*rowsDL, error) {
	client, ok := cfg.Athena.(*athena.Client)
	if !ok {
		return nil, fmt.Errorf("invalid athena client type")
	}
	r := &rowsDL{
		athena:     client,
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

	errChan := make(chan error, 2)
	// download and set in memory
	go r.downloadCsvAsync(ctx, errChan, cfg.OutputLocation)
	// get table metadata
	go r.getQueryResultsAsyncForCsv(ctx, errChan)

	for i := 0; i < 2; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errChan:
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *rowsDL) Columns() []string {
	return r.columnNames
}

func (r *rowsDL) Close() error {
	return nil
}

func (r *rowsDL) Next(dest []driver.Value) error {
	if r.recordPtr >= len(r.records) {
		return io.EOF
	}

	record := r.records[r.recordPtr]
	r.recordPtr++

	for i := range dest {
		if i >= len(record) {
			dest[i] = nil
			continue
		}

		if record[i].isNil {
			dest[i] = nil
			continue
		}

		v, err := r.columnTypes[i].ConvertValue(record[i].val)
		if err != nil {
			return err
		}
		dest[i] = v
	}

	return nil
}

func (r *rowsDL) ColumnTypeDatabaseTypeName(index int) string {
	return r.columnTypes[index].DatabaseTypeName()
}

func (r *rowsDL) downloadCsvAsync(ctx context.Context, errChan chan<- error, outputLocation string) {
	defer func() {
		errChan <- nil
	}()

	u, err := url.Parse(outputLocation)
	if err != nil {
		errChan <- fmt.Errorf("failed to parse output location: %v", err)
		return
	}

	key := strings.TrimPrefix(u.Path, "/")
	key = fmt.Sprintf("%s/%s.csv", key, r.queryID)

	// Download CSV file
	input := &s3.GetObjectInput{
		Bucket: aws.String(u.Host),
		Key:    aws.String(key),
	}

	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		errChan <- fmt.Errorf("failed to load AWS config: %v", err)
		return
	}

	s3Client := s3.NewFromConfig(cfg)
	resp, err := s3Client.GetObject(ctx, input)
	if err != nil {
		errChan <- fmt.Errorf("failed to download CSV: %v", err)
		return
	}
	defer resp.Body.Close()

	// Parse CSV
	records, err := getRecordsForDL(resp.Body)
	if err != nil {
		errChan <- fmt.Errorf("failed to parse CSV: %v", err)
		return
	}

	r.records = records
}

func (r *rowsDL) getQueryResultsAsyncForCsv(ctx context.Context, errChan chan<- error) {
	defer func() {
		errChan <- nil
	}()

	input := &athena.GetQueryResultsInput{
		QueryExecutionId: aws.String(r.queryID),
	}

	resp, err := r.athena.GetQueryResults(ctx, input)
	if err != nil {
		errChan <- fmt.Errorf("failed to get query results: %v", err)
		return
	}

	if resp.ResultSet == nil || resp.ResultSet.ResultSetMetadata == nil {
		errChan <- fmt.Errorf("invalid response format")
		return
	}

	columnInfo := resp.ResultSet.ResultSetMetadata.ColumnInfo
	r.columnNames = make([]string, len(columnInfo))
	r.columnTypes = make([]*columnType, len(columnInfo))

	for i, info := range columnInfo {
		r.columnNames[i] = *info.Name
		r.columnTypes[i] = newColumnType(*info.Type)
	}
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
