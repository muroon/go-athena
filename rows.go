package athena

import (
	"bytes"
	"database/sql/driver"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
)

// writeAtBuffer implements io.WriterAt for a buffer
type writeAtBuffer struct {
	buf *bytes.Buffer
}

func (w *writeAtBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	// Note that we ignore the offset as we are just appending
	return w.buf.Write(p)
}

type rowsConfig struct {
	Athena         *athena.Client
	QueryID        string
	SkipHeader     bool
	ResultMode     ResultMode
	AWSConfig      *aws.Config
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
