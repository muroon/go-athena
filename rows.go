package athena

import (
	"database/sql/driver"
)

type rowsConfig struct {
	Athena interface {
		GetQueryResultsAPI
		StartQueryExecutionAPI
		GetWorkGroupAPI
		StopQueryExecutionAPI
		GetQueryExecutionAPI
	}
	QueryID        string
	SkipHeader     bool
	ResultMode     ResultMode
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
	default:
		r, err = newRowsAPI(cfg)
	}

	return r, err
}
