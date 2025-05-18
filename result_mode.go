package athena

// ResultMode Results mode
type ResultMode int

const (
	// ResultModeAPI api access Mode
	ResultModeAPI ResultMode = 0

	// ResultModeDL download results Mode
	ResultModeDL ResultMode = 1

	// ResultModeGzipDL ctas query and download gzip file Mode
	ResultModeGzipDL ResultMode = 2

	// ResultModeParquetDL ctas query and download parquet file Mode
	ResultModeParquetDL ResultMode = 3
)
