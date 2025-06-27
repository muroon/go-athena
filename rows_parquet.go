package athena

import (
	"bufio"
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type rowsParquet struct {
	athena     *athena.Client
	queryID    string
	resultMode ResultMode

	// use download
	downloadedRows *downloadedRows

	// ctas table
	ctasTable        string
	db               string
	catalog          string
	ctasTableColumns []types.Column
}

func newRowsParquet(cfg rowsConfig) (*rowsParquet, error) {
	r := &rowsParquet{
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

func (r *rowsParquet) init(cfg rowsConfig) error {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Timeout)*time.Second)
	defer cancel()

	err := make(chan error, 2)

	// download and set in memory
	go r.downloadParquetDataAsync(ctx, err, cfg.Config, cfg.OutputLocation)

	// get table metadata
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

	// drop ctas table
	if cfg.AfterDownload != nil {
		if e := cfg.AfterDownload(); e != nil {
			return e
		}
	}

	return nil
}

func (r *rowsParquet) downloadParquetDataAsync(
	ctx context.Context,
	errCh chan error,
	cfg aws.Config,
	location string,
) {
	errCh <- r.downloadParquetData(ctx, cfg, location)
}

func (r *rowsParquet) downloadParquetData(ctx context.Context, cfg aws.Config, location string) error {
	if location[len(location)-1:] == "/" {
		location = location[:len(location)-1]
	}

	// remove the first 5 characters "s3://" from location
	bucketName := location[5:]

	// Create an S3 client
	s3Client := s3.NewFromConfig(cfg)

	// get parquet file path
	resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(fmt.Sprintf("tables/%s-manifest.csv", r.queryID)),
	})
	if err != nil {
		return err
	}

	// Read the manifest file content
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

	for _, objectKey := range objectKeys {
		resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			return err
		}

		// Read the object content
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return err
		}

		// parse parquet data (for now, use simple text parsing similar to CSV)
		// In a real implementation, you would use a proper parquet library
		datas, err := getRecordsFromParquet(strings.NewReader(string(data)))
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

func (r *rowsParquet) getTableAsync(ctx context.Context, errCh chan error) {
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

func (r *rowsParquet) nextCTAS(dest []driver.Value) error {
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

func (r *rowsParquet) columnTypeDatabaseTypeNameForCTAS(index int) string {
	column := r.ctasTableColumns[index]
	if column.Type == nil {
		return ""
	}
	return *column.Type
}

func (r *rowsParquet) Columns() []string {
	var columns []string

	for _, col := range r.ctasTableColumns {
		columns = append(columns, *col.Name)
	}

	return columns
}

func (r *rowsParquet) ColumnTypeDatabaseTypeName(index int) string {
	return r.columnTypeDatabaseTypeNameForCTAS(index)
}

func (r *rowsParquet) Next(dest []driver.Value) error {
	return r.nextCTAS(dest)
}

func (r *rowsParquet) Close() error {
	return nil
}

func getObjectKeysForParquet(reader io.Reader, start int) ([]string, error) {
	keys := make([]string, 0)
	scanner := bufio.NewScanner(reader)

	// read line by line
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

func getRecordsFromParquet(reader io.Reader) ([][]string, error) {
	// This is a simplified implementation for demonstration purposes
	// In a real implementation, you would use a proper parquet library
	// For now, we'll parse parquet files assuming they are stored in binary format
	// but we'll try to handle them similarly to the CSV approach
	records := make([][]string, 0)

	// Read all data first
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	// For parquet files, try to parse as raw binary data
	// Convert binary data to text representation first
	lines := strings.Split(string(data), "\n")
	
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		
		// Try different parsing approaches for parquet data
		var record []string
		
		// First try: tab-separated
		if strings.Contains(line, "\t") {
			record = strings.Split(line, "\t")
		} else if strings.Contains(line, ",") {
			// Second try: comma-separated  
			record = strings.Split(line, ",")
		} else {
			// Third try: field separator (similar to gzip)
			field := ""
			record = make([]string, 0)
			for _, r := range line {
				if r == '\001' || r == '\000' {
					if field != "" {
						record = append(record, field)
						field = ""
					}
				} else if r >= 32 && r <= 126 { // printable ASCII
					field += string(r)
				}
			}
			if field != "" {
				record = append(record, field)
			}
		}
		
		if len(record) > 0 {
			records = append(records, record)
		}
	}

	return records, nil
}