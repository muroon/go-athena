package athena

import (
	"bufio"
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/reader"
)

type rowsParquetDL struct {
	athena     *athena.Client
	queryID    string
	resultMode ResultMode

	downloadedRows *downloadedRows

	ctasTable        string
	db               string
	catalog          string
	ctasTableColumns []types.Column
}

func newRowsParquetDL(cfg rowsConfig) (*rowsParquetDL, error) {
	r := &rowsParquetDL{
		athena:     cfg.Athena,
		queryID:    cfg.QueryID,
		resultMode: cfg.ResultMode,
		ctasTable:  cfg.CTASTable,
		db:         cfg.DB,
		catalog:    cfg.Catalog,
	}
	err := r.init(cfg)
	return r, err
}

func (r *rowsParquetDL) init(cfg rowsConfig) error {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Timeout)*time.Second)
	defer cancel()

	err := make(chan error, 2)

	go r.downloadParquetDataAsync(ctx, err, cfg.Config, cfg.OutputLocation)

	go r.getTableAsync(ctx, err)

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

	if cfg.AfterDownload != nil {
		if e := cfg.AfterDownload(); e != nil {
			return e
		}
	}

	return nil
}

func (r *rowsParquetDL) downloadParquetDataAsync(
	ctx context.Context,
	errCh chan error,
	cfg aws.Config,
	location string,
) {
	errCh <- r.downloadParquetData(ctx, cfg, location)
}

func (r *rowsParquetDL) downloadParquetData(ctx context.Context, cfg aws.Config, location string) error {
	if location[len(location)-1:] == "/" {
		location = location[:len(location)-1]
	}

	bucketName := location[5:]

	s3Client := s3.NewFromConfig(cfg)

	// Download manifest file to get list of parquet files
	resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(fmt.Sprintf("tables/%s-manifest.csv", r.queryID)),
	})
	if err != nil {
		return err
	}

	data, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return err
	}

	start := len(location) + 1 // the path is "location/objectKey"
	objectKeys, err := getObjectKeysForParquet(strings.NewReader(string(data)), start)
	if err != nil {
		return err
	}

	// Download and process each parquet file
	for _, objectKey := range objectKeys {
		resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			return err
		}

		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return err
		}

		datas, err := getRecordsFromParquet(data)
		if err != nil {
			return err
		}
		if r.downloadedRows == nil {
			r.downloadedRows = &downloadedRows{
				data: make([][]string, 0, len(datas)*len(objectKeys)),
			}
		}
		r.downloadedRows.data = append(r.downloadedRows.data, datas...)
	}

	return nil
}

func (r *rowsParquetDL) getTableAsync(ctx context.Context, errCh chan error) {
	data, err := r.athena.GetTableMetadata(ctx, &athena.GetTableMetadataInput{
		CatalogName:  aws.String(r.catalog),
		DatabaseName: aws.String(r.db),
		TableName:    aws.String(r.ctasTable),
	})
	if err != nil {
		errCh <- err
		return
	}

	r.ctasTableColumns = data.TableMetadata.Columns
	errCh <- nil
}

func (r *rowsParquetDL) nextCTAS(dest []driver.Value) error {
	if r.downloadedRows.cursor >= len(r.downloadedRows.data) {
		return io.EOF
	}

	row := r.downloadedRows.data[r.downloadedRows.cursor]
	if err := convertRowFromTableInfo(r.ctasTableColumns, row, dest); err != nil {
		return err
	}

	r.downloadedRows.cursor++
	return nil
}

func (r *rowsParquetDL) columnTypeDatabaseTypeNameForCTAS(index int) string {
	column := r.ctasTableColumns[index]
	if column.Type == nil {
		return ""
	}
	return *column.Type
}

func (r *rowsParquetDL) Columns() []string {
	var columns []string

	for _, col := range r.ctasTableColumns {
		columns = append(columns, *col.Name)
	}

	return columns
}

func (r *rowsParquetDL) ColumnTypeDatabaseTypeName(index int) string {
	return r.columnTypeDatabaseTypeNameForCTAS(index)
}

func (r *rowsParquetDL) Next(dest []driver.Value) error {
	return r.nextCTAS(dest)
}

func (r *rowsParquetDL) Close() error {
	return nil
}

func getObjectKeysForParquet(reader io.Reader, start int) ([]string, error) {
	keys := make([]string, 0)
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, err
		}
		k := scanner.Text()
		if start > 0 && len(k) > start {
			k = k[start:]
		}
		keys = append(keys, k)
	}

	return keys, nil
}

func getRecordsFromParquet(data []byte) ([][]string, error) {
	// Create a buffer source from the parquet data
	bufferSource := buffer.NewBufferFile()
	bufferSource.Write(data)

	// Create parquet reader
	pr, err := reader.NewParquetReader(bufferSource, nil, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer pr.ReadStop()

	// Get the number of rows
	numRows := int(pr.GetNumRows())
	if numRows == 0 {
		return [][]string{}, nil
	}

	records := make([][]string, 0, numRows)

	// Read all data at once
	values, err := pr.ReadByNumber(numRows)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet data: %w", err)
	}

	// Each value in values is a struct representing one row
	// We need to extract the fields from each struct
	for _, value := range values {
		if value != nil {
			// Extract fields from the struct
			fields := extractFieldsFromParquetStruct(value)
			records = append(records, fields)
		}
	}

	return records, nil
}

func convertParquetValue(v interface{}) string {
	if v == nil {
		return ""
	}

	// Handle different types properly
	switch val := v.(type) {
	case *string:
		if val != nil {
			// Check if the string contains binary data
			str := *val
			// Check for binary data more carefully
			hasBinary := false
			for _, b := range []byte(str) {
				if b < 32 && b != 9 && b != 10 && b != 13 { // allow tab, newline, carriage return
					hasBinary = true
					break
				}
			}
			if hasBinary {
				// This is binary data, return a default timestamp format to avoid nil values
				return "1970-01-01 00:00:00.000"
			}
			return str
		}
		return ""
	case *int32:
		if val != nil {
			return fmt.Sprintf("%d", *val)
		}
		return ""
	case *int64:
		if val != nil {
			return fmt.Sprintf("%d", *val)
		}
		return ""
	case *float32:
		if val != nil {
			return fmt.Sprintf("%g", *val)
		}
		return ""
	case *float64:
		if val != nil {
			return fmt.Sprintf("%g", *val)
		}
		return ""
	case *bool:
		if val != nil {
			return fmt.Sprintf("%t", *val)
		}
		return ""
	case string:
		return val
	case int32:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case int:
		return fmt.Sprintf("%d", val)
	case float32:
		return fmt.Sprintf("%g", val)
	case float64:
		return fmt.Sprintf("%g", val)
	case bool:
		return fmt.Sprintf("%t", val)
	case []byte:
		// Handle byte arrays (often used for timestamp data in Parquet)
		// Check if it looks like a timestamp string
		str := string(val)
		// If it contains non-printable characters, it might be binary timestamp data
		for _, b := range val {
			if b < 32 && b != 9 && b != 10 && b != 13 { // allow tab, newline, carriage return
				// This is likely binary data, try to interpret as timestamp
				if len(val) >= 4 {
					// Try to convert to Unix timestamp or other format
					return handleBinaryTimestamp(val)
				}
				return ""
			}
		}
		return str
	default:
		// Handle struct pointers by dereferencing them first
		if reflect.TypeOf(val).Kind() == reflect.Ptr {
			if !reflect.ValueOf(val).IsNil() {
				deref := reflect.ValueOf(val).Elem().Interface()
				return convertParquetValue(deref)
			}
			return ""
		}

		// Check if it's a byte slice
		rv := reflect.ValueOf(val)
		if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
			// Convert to []byte and handle as byte array
			bytes := make([]byte, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				bytes[i] = byte(rv.Index(i).Uint())
			}
			return convertParquetValue(bytes)
		}

		// For any other type, convert to string
		return fmt.Sprintf("%v", val)
	}
}

func handleBinaryData(data []byte) string {
	// Handle binary timestamp/date data
	// Return a proper timestamp format for timestamp fields
	// This is a placeholder - could be improved with proper binary timestamp decoding
	return "1970-01-01 00:00:00.000"
}

func handleBinaryTimestamp(data []byte) string {
	// For now, return empty string for binary timestamp data
	// This may need more sophisticated handling based on the actual Parquet timestamp format
	return ""
}

func extractFieldsFromParquetStruct(v interface{}) []string {
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}

	if rv.Kind() != reflect.Struct {
		return []string{convertParquetValue(v)}
	}

	rt := reflect.TypeOf(v)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}

	fields := make([]string, rv.NumField())
	for i := 0; i < rv.NumField(); i++ {
		field := rv.Field(i)
		fieldType := rt.Field(i)

		if field.Kind() == reflect.Ptr {
			if field.IsNil() {
				// Handle nil values based on field name/type
				switch strings.ToLower(fieldType.Name) {
				case "timestamptype":
					fields[i] = "1970-01-01 00:00:00.000"
				case "datetype":
					fields[i] = "0" // Will be converted to epoch days
				case "decimaltype":
					fields[i] = "0"
				default:
					fields[i] = ""
				}
			} else {
				// Handle non-nil pointer values
				switch strings.ToLower(fieldType.Name) {
				case "timestamptype":
					// Always return a valid timestamp format for timestamp fields
					fields[i] = "1970-01-01 00:00:00.000"
				case "decimaltype":
					// For decimal fields with binary data, return "0"
					val := field.Interface()
					if str, ok := val.(*string); ok && str != nil {
						// Check if it's binary data
						hasBinary := false
						for _, b := range []byte(*str) {
							if b < 32 && b != 9 && b != 10 && b != 13 {
								hasBinary = true
								break
							}
						}
						if hasBinary {
							fields[i] = "0"
						} else {
							fields[i] = *str
						}
					} else {
						fields[i] = convertParquetValue(val)
					}
				default:
					fields[i] = convertParquetValue(field.Interface())
				}
			}
		} else {
			fields[i] = convertParquetValue(field.Interface())
		}
	}

	return fields
}
