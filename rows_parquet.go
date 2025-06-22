package athena

import (
	"bufio"
	"bytes"
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"time"

	"compress/gzip"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type rowsParquetDL struct {
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

		// For Parquet files, we need to use a different approach
		// Since Go doesn't have built-in Parquet support, we'll use CSV conversion
		// In practice, you might want to use a Parquet library like xitongsys/parquet-go
		// For now, we'll assume the data is in a text format similar to the GZIP approach
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
	records := make([][]string, 0)
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	// バイナリデータの処理
	if len(data) == 0 {
		return records, nil
	}

	// バイナリデータをテキストに変換
	text := string(data)
	if strings.HasPrefix(text, "\x00") {
		// バイナリデータをスキップして次の有効なデータを探す
		validStart := 0
		for i := 0; i < len(text); i++ {
			if text[i] != 0 && text[i] != '\x15' && text[i] != '\x04' {
				validStart = i
				break
			}
		}
		text = text[validStart:]
	}

	// GZIPデータの処理
	if strings.HasPrefix(text, "\x1f\x8b") {
		reader := bytes.NewReader([]byte(text))
		gzipReader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
		defer gzipReader.Close()

		data, err = io.ReadAll(gzipReader)
		if err != nil {
			return nil, err
		}
		text = string(data)
	}

	lines := strings.Split(text, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		// タブ区切りでフィールドを分割
		fields := strings.Split(line, "\t")
		// NULL値の処理
		for i, field := range fields {
			if field == nullStringResultModeParquetDL {
				fields[i] = ""
			}
		}
		records = append(records, fields)
	}

	return records, nil
}
