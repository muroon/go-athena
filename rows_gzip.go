package athena

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type rowsGzipDL struct {
	athena     *athena.Client
	queryID    string
	resultMode ResultMode

	downloadedRows   *downloadedRows
	ctasTable        string
	db               string
	catalog          string
	ctasTableColumns []types.Column
}

func newRowsGzipDL(cfg rowsConfig) (*rowsGzipDL, error) {
	r := &rowsGzipDL{
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

func (r *rowsGzipDL) init(cfg rowsConfig) error {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Timeout)*time.Second)
	defer cancel()

	err := make(chan error, 2)

	// download and set in memory
	go r.downloadCompressedDataAsync(ctx, err, cfg.AWSConfig, cfg.OutputLocation)

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

func (r *rowsGzipDL) downloadCompressedDataAsync(
	ctx context.Context,
	errCh chan error,
	cfg *aws.Config,
	location string,
) {
	errCh <- r.downloadCompressedData(cfg, location)
}

func (r *rowsGzipDL) downloadCompressedData(cfg *aws.Config, location string) error {
	if location[len(location)-1:] == "/" {
		location = location[:len(location)-1]
	}

	bucketName := location[5:]

	s3Client := s3.NewFromConfig(*cfg)
	downloader := manager.NewDownloader(s3Client)

	var buf bytes.Buffer
	_, err := downloader.Download(context.Background(), &writeAtBuffer{buf: &buf}, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(fmt.Sprintf("tables/%s-manifest.csv", r.queryID)),
	})
	if err != nil {
		return err
	}

	start := len(location) + 1
	objectKeys, err := getObjectKeysForGzip(strings.NewReader(buf.String()), start)
	if err != nil {
		return err
	}

	for _, objectKey := range objectKeys {
		var buf bytes.Buffer
		_, err := downloader.Download(context.Background(), &writeAtBuffer{buf: &buf}, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			return err
		}

		gzipReader, err := gzip.NewReader(strings.NewReader(buf.String()))
		if err != nil {
			return err
		}

		datas, err := getRecordsFromGzip(gzipReader)
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

func (r *rowsGzipDL) getTableAsync(ctx context.Context, errCh chan error) {
	input := &athena.GetTableMetadataInput{
		CatalogName:  aws.String(r.catalog),
		DatabaseName: aws.String(r.db),
		TableName:    aws.String(r.ctasTable),
	}

	output, err := r.athena.GetTableMetadata(ctx, input)
	if err != nil {
		errCh <- err
		return
	}

	r.ctasTableColumns = output.TableMetadata.Columns
	errCh <- nil
}

func (r *rowsGzipDL) nextCTAS(dest []driver.Value) error {
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

func (r *rowsGzipDL) columnTypeDatabaseTypeNameForCTAS(index int) string {
	if index >= len(r.ctasTableColumns) {
		return ""
	}
	column := r.ctasTableColumns[index]
	if column.Type == nil {
		return ""
	}
	return *column.Type
}

func (r *rowsGzipDL) Columns() []string {
	var columns []string

	for _, col := range r.ctasTableColumns {
		columns = append(columns, *col.Name)
	}

	return columns
}

func (r *rowsGzipDL) ColumnTypeDatabaseTypeName(index int) string {
	return r.columnTypeDatabaseTypeNameForCTAS(index)
}

func (r *rowsGzipDL) Next(dest []driver.Value) error {
	return r.nextCTAS(dest)
}

func (r *rowsGzipDL) Close() error {
	return nil
}

func getObjectKeysForGzip(reader io.Reader, start int) ([]string, error) {

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

func getRecordsFromGzip(reader io.Reader) ([][]string, error) {
	records := make([][]string, 0)

	scanner := bufio.NewScanner(reader)

	// read line by line
	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, err
		}
		b := scanner.Bytes()
		field := ""
		record := make([]string, 0)
		for {
			r, width := utf8.DecodeRune(b)
			if r == '\001' {
				record = append(record, field)
				field = ""
			} else {
				field += string(r)
			}
			if width >= len(b) {
				record = append(record, field)
				break
			}
			b = b[width:]
		}

		records = append(records, record)
	}

	return records, nil
}
