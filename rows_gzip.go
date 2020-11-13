package athena

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"strings"
	"time"
)

const (
	CATALOG_AWS_DATA_CATALOG string = "AwsDataCatalog"
)

type rowsGzipDL struct {
	athena  athenaiface.AthenaAPI
	queryID string
	resultMode ResultMode

	// use download
	downloadedRows *downloadedRows

	// ctas table
	ctasTable string
	db        string
	catalog   string
	ctasTableColumns []*athena.Column
}

func newRowsGzipDL(cfg rowsConfig) (*rowsGzipDL, error) {
	r := &rowsGzipDL{
		athena:        cfg.Athena,
		queryID:       cfg.QueryID,
		resultMode:    cfg.ResultMode,
		ctasTable:     cfg.CTASTable,
		db:            cfg.DB,
		catalog:       cfg.Catalog,
	}
	err := r.init(cfg)
	return r, err
}

func (r *rowsGzipDL) init(cfg rowsConfig) error {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, time.Duration(cfg.Timeout) * time.Second)
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

	bfData := buff.Bytes()
	datas, err := getAsCsv(bfData)
	if err != nil {
		return err
	}

	objectKeys := make([]string, 0, len(datas))
	for _, d := range datas {
		objectKey := d[0][len(location)+1:]
		objectKeys = append(objectKeys, objectKey)
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
		output := bytes.Buffer{}
		output.ReadFrom(gzipReader)

		datas, err := getAsCsv(output.Bytes())
		if err != nil {
			return err
		}
		if r.downloadedRows == nil {
			r.downloadedRows = &downloadedRows{
				header: datas[0],
				data: make([][]string, 0, len(datas)* len(objectKeys)),
			}
		}
		r.downloadedRows.data = append(r.downloadedRows.data, datas...)
	}

	return nil
}

func (r *rowsGzipDL) getTableAsync(ctx context.Context, errCh chan error) {
	data, err := r.athena.GetTableMetadata(&athena.GetTableMetadataInput{
		CatalogName: aws.String(r.catalog),
		DatabaseName: aws.String(r.db),
		TableName: aws.String(r.ctasTable),
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

	func getGZPaths(sess client.ConfigProvider, bucket, queryID string) ([]string, error) {
	gzFilePaths := make([]string, 0)

	svc := s3.New(sess)

	prefix := fmt.Sprintf("tables/%s", queryID)
	params := &s3.ListObjectsV2Input{
		Bucket: &bucket,
		Prefix: &prefix,
	}

	err := svc.ListObjectsV2Pages(params,
		func(page *s3.ListObjectsV2Output, lastPage bool) bool {
			for _, obj := range page.Contents {
				objKey := *obj.Key
				start := len(objKey)-3
				if objKey[start:] == ".gz" {
					gzFilePaths = append(gzFilePaths, objKey)
				}
			}

			return *page.IsTruncated
		})

	return gzFilePaths, err
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
