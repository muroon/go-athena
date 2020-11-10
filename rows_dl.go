package athena

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"io"
	"strings"
)


func (r *rows) isDownload() bool {
	return r.mode == modeDownload
}

func (r *rows) downloadCsvAsync(
	ctx context.Context,
	errCh chan error,
	sess *session.Session,
	location string,
) {
	errCh <- r.downloadCsv(sess, location)
}

func (r *rows) downloadCsv(sess *session.Session, location string) error {
	// remove the first 5 characters "s3://" from location
	bucketName := location[5:]
	objectKey := fmt.Sprintf("%s.csv", r.queryID)

	buff := &aws.WriteAtBuffer{}
	downloader := s3manager.NewDownloader(sess)
	_, err := downloader.Download(buff, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return err
	}

	bfData := buff.Bytes()

	datas, err := getAsCsv(bfData)
	if err != nil {
		return err
	}
	r.downloadedRows = &downloadedRows{
		header: datas[0],
		data: datas[1:],
	}

	return nil
}

func (r *rows) getQueryResultsAsyncForCsv(ctx context.Context, errCh chan error) {
	var err error
	r.out, err = r.athena.GetQueryResults(&athena.GetQueryResultsInput{
		QueryExecutionId: aws.String(r.queryID),
		MaxResults: aws.Int64(1),
	})
	errCh <- err
}

func getAsCsv(data []byte) ([][]string, error) {
	records := make([][]string, 0)
	r := csv.NewReader(strings.NewReader(string(data)))
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		records = append(records, record)
	}
	return records, nil
}
