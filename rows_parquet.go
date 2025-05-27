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
	records := make([][]string, 0, numRows)

	// Read all records
	rowsToRead := numRows
	if rowsToRead > 10000 {
		// Process in batches for large files
		return readParquetInBatches(pr, numRows)
	}

	// Read all rows at once for smaller files
	values, err := pr.ReadByNumber(rowsToRead)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet data: %w", err)
	}

	// Convert values to string records
	numCols := len(pr.Footer.Schema) - 1 // Exclude root schema
	for i := 0; i < numRows; i++ {
		record := make([]string, numCols)
		for j := 0; j < numCols; j++ {
			idx := i*numCols + j
			if idx < len(values) && values[idx] != nil {
				record[j] = fmt.Sprintf("%v", values[idx])
			} else {
				record[j] = ""
			}
		}
		records = append(records, record)
	}

	return records, nil
}

func readParquetInBatches(pr *reader.ParquetReader, totalRows int) ([][]string, error) {
	batchSize := 1000
	records := make([][]string, 0, totalRows)
	numCols := len(pr.Footer.Schema) - 1 // Exclude root schema

	for remaining := totalRows; remaining > 0; {
		readSize := batchSize
		if remaining < batchSize {
			readSize = remaining
		}

		values, err := pr.ReadByNumber(readSize)
		if err != nil {
			return nil, fmt.Errorf("failed to read parquet batch: %w", err)
		}

		// Convert batch to string records
		batchRows := readSize
		for i := 0; i < batchRows; i++ {
			record := make([]string, numCols)
			for j := 0; j < numCols; j++ {
				idx := i*numCols + j
				if idx < len(values) && values[idx] != nil {
					record[j] = fmt.Sprintf("%v", values[idx])
				} else {
					record[j] = ""
				}
			}
			records = append(records, record)
		}

		remaining -= readSize
	}

	return records, nil
}
