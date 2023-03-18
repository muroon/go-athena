package athena

import (
	"bufio"
	"compress/gzip"
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	CATALOG_AWS_DATA_CATALOG string = "AwsDataCatalog"
)

type rowsGzipDL struct {
	athena     athenaiface.AthenaAPI
	queryID    string
	resultMode ResultMode

	// use download
	downloadedRows *downloadedRows

	// ctas table
	ctasTable        string
	db               string
	catalog          string
	ctasTableColumns []*athena.Column
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
	go r.downloadCompressedDataAsync(ctx, err, cfg.Session, cfg.OutputLocation)

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
	// TODO: remove this
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
	sess *session.Session,
	location string,
) {
	errCh <- r.downloadCompressedData(sess, location)
}

func (r *rowsGzipDL) downloadCompressedData(sess *session.Session, location string) error {
	if location[len(location)-1:] == "/" {
		location = location[:len(location)-1]
	}

	// remove the first 5 characters "s3://" from location
	bucketName := location[5:]

	// get gz file path
	buff := &aws.WriteAtBuffer{}

	downloader := s3manager.NewDownloader(sess)
	_, err := downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(fmt.Sprintf("tables/%s-manifest.csv", r.queryID)),
	})
	if err != nil {
		return err
	}

	start := len(location) + 1 // the path is "location/objectKey"
	objectKeys, err := getObjectKeysForGzip(strings.NewReader(string(buff.Bytes())), start)
	if err != nil {
		return err
	}

	for _, objectKey := range objectKeys {
		buff := &aws.WriteAtBuffer{}

		_, err := downloader.Download(buff, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			return err
		}

		bfData := buff.Bytes()

		// decompress gzip
		gzipReader, err := gzip.NewReader(strings.NewReader(string(bfData)))
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
	data, err := r.athena.GetTableMetadata(&athena.GetTableMetadataInput{
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
	column := r.ctasTableColumns[index]
	if column == nil || column.Type == nil {
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
