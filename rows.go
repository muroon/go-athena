package athena

import (
	"database/sql/driver"
	"encoding/csv"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
	"io"
	"strings"
)

type rowsConfig struct {
	Athena     athenaiface.AthenaAPI
	QueryID    string
	SkipHeader bool
	ResultMode ResultMode
	Session    *session.Session
	OutputLocation string
	Timeout uint
	AfterDownload func() error
	CTASTable string
	DB string
	Catalog string
}

type downloadedRows struct {
	cursor int
	header []string
	data [][]string
}

func newRows(cfg rowsConfig) (driver.Rows, error) {
	var r driver.Rows
	var err error
	switch cfg.ResultMode {
	case ResultModeDL:
		r, err = newRowsDL(cfg)
	case ResultModeGzipDL:
		r, err = newRowsGzipDL(cfg)
	default:
		r, err = newRowsAPI(cfg)
	}

	return r, err
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
