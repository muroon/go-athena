package athena

import (
	"database/sql/driver"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
)

type rowsConfig struct {
	Athena         *athena.Client
	QueryID        string
	SkipHeader     bool
	ResultMode     ResultMode
	Config         aws.Config
	OutputLocation string
	Timeout        uint
	AfterDownload  func() error
	CTASTable      string
	DB             string
	Catalog        string
}

type downloadedRows struct {
	cursor int
	data   [][]string        // for gzip dl
	field  [][]downloadField // for csv dl
}

type downloadField struct {
	val   string
	isNil bool
}

func newRows(cfg rowsConfig) (driver.Rows, error) {
	var r driver.Rows
	var err error
	switch cfg.ResultMode {
	case ResultModeDL:
		r, err = newRowsDL(cfg)
	case ResultModeGzipDL:
		r, err = newRowsGzipDL(cfg)
	case ResultModeParquetDL:
		r, err = newRowsParquetDL(cfg)
	default:
		r, err = newRowsAPI(cfg)
	}

	return r, err
}
