package athena

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	AthenaDatabase     = "go_athena_tests"
	S3Bucket           = "go-athena-tests"
	AwsRegion          = "us-east-1"
	WorkGroup          = "primary"
	AutoOutputLocation = false
)

func init() {
	if v := os.Getenv("ATHENA_DATABASE"); v != "" {
		AthenaDatabase = v
	}

	if v := os.Getenv("S3_BUCKET"); v != "" {
		S3Bucket = v
	}

	if v := os.Getenv("AWS_DEFAULT_REGION"); v != "" {
		AwsRegion = v
	}
	if v := os.Getenv("ATHENA_REGION"); v != "" {
		AwsRegion = v
	}
	if v := os.Getenv("ATHENA_WORK_GROUP"); v != "" {
		WorkGroup = v
	}
	AutoOutputLocation = os.Getenv("ATHENA_AUTO_OUTPUT_LOCATION") == "1"
}

func TestQuery(t *testing.T) {
	harness := setup(t)
	defer harness.teardown()

	expected := []dummyRow{
		{
			SmallintType:  1,
			IntType:       2,
			BigintType:    3,
			BooleanType:   true,
			FloatType:     3.14159,
			DoubleType:    1.32112345,
			StringType:    "some string",
			TimestampType: athenaTimestamp(time.Date(2006, 1, 2, 3, 4, 11, 0, time.UTC)),
			DateType:      athenaDate(time.Date(2006, 1, 2, 0, 0, 0, 0, time.UTC)),
			DecimalType:   1001,
		},
		{
			SmallintType:  9,
			IntType:       8,
			BigintType:    0,
			BooleanType:   false,
			FloatType:     3.14159,
			DoubleType:    1.235,
			StringType:    "another string",
			TimestampType: athenaTimestamp(time.Date(2017, 12, 3, 1, 11, 12, 0, time.UTC)),
			DateType:      athenaDate(time.Date(2017, 12, 3, 0, 0, 0, 0, time.UTC)),
			DecimalType:   0,
		},
		{
			SmallintType:  9,
			IntType:       8,
			BigintType:    0,
			BooleanType:   false,
			DoubleType:    1.235,
			FloatType:     3.14159,
			StringType:    "another string",
			TimestampType: athenaTimestamp(time.Date(2017, 12, 3, 20, 11, 12, 0, time.UTC)),
			DateType:      athenaDate(time.Date(2017, 12, 3, 0, 0, 0, 0, time.UTC)),
			DecimalType:   0.48,
		},
	}
	expectedTypeNames := []string{"varchar", "smallint", "integer", "bigint", "boolean", "float", "double", "varchar", "timestamp", "date", "decimal"}
	expectedTypeNameGzipDLs := []string{"string", "smallint", "int", "bigint", "boolean", "float", "double", "string", "timestamp", "date", "decimal(11,5)"}
	harness.uploadData(expected)

	resultModes := []ResultMode{
		ResultModeAPI,
		ResultModeDL,
		ResultModeGzipDL,
	}

	for _, resultMode := range resultModes {
		ctx := context.Background()
		switch resultMode {
		case ResultModeAPI:
			ctx = SetAPIMode(ctx)
		case ResultModeDL:
			ctx = SetDLMode(ctx)
		case ResultModeGzipDL:
			ctx = SetGzipDLMode(ctx)
		}

		rows := harness.mustQuery(ctx, "select * from %s", harness.table)
		index := -1
		for rows.Next() {
			index++

			var row dummyRow
			require.NoError(t, rows.Scan(
				&row.NullValue,

				&row.SmallintType,
				&row.IntType,
				&row.BigintType,
				&row.BooleanType,
				&row.FloatType,
				&row.DoubleType,
				&row.StringType,
				&row.TimestampType,
				&row.DateType,
				&row.DecimalType,
			))

			assert.Equal(t, expected[index], row, fmt.Sprintf("resultMode:%v, index:%d", resultMode, index))

			types, err := rows.ColumnTypes()
			assert.NoError(t, err, fmt.Sprintf("resultMode:%v, index:%d", resultMode, index))

			etns := expectedTypeNames
			if resultMode == ResultModeGzipDL {
				etns = expectedTypeNameGzipDLs
			}
			for i, colType := range types {
				typeName := colType.DatabaseTypeName()
				assert.Equal(t, etns[i], typeName, fmt.Sprintf("resultMode:%v, index:%d", resultMode, index))
			}
		}

		require.NoError(t, rows.Err(), fmt.Sprintf("rows.Err(). resultMode:%v", resultMode))
		require.Equal(t, 3, index+1, fmt.Sprintf("row count. resultMode:%v", resultMode))
	}
}

func TestOpen(t *testing.T) {
	var acfg []*aws.Config
	acfg = append(acfg, &aws.Config{Region: aws.String(AwsRegion)})
	session, err := session.NewSession(acfg...)
	require.NoError(t, err, "Query")

	resultModes := []ResultMode{
		ResultModeAPI,
		ResultModeDL,
		ResultModeGzipDL,
	}

	config := Config{
		Session:   session,
		Database:  AthenaDatabase,
		WorkGroup: WorkGroup,
		Timeout:   timeOutLimitDefault,
	}
	if !AutoOutputLocation {
		config.OutputLocation = fmt.Sprintf("s3://%s", S3Bucket)
	}

	for _, resultMode := range resultModes {
		config.ResultMode = resultMode
		db, err := Open(config)
		require.NoError(t, err, fmt.Sprintf("Open. resultMode:%v", resultMode))

		ctx := context.Background()
		_, err = db.QueryContext(ctx, "SELECT 1")
		if resultMode == ResultModeGzipDL {
			require.Error(t, err, "Query IN Gzip DL Mode")
		} else {
			require.NoError(t, err, fmt.Sprintf("Query IN resultMode:%v", resultMode))
		}
	}
}

func TestDDLQuery(t *testing.T) {
	harness := setup(t)
	defer harness.teardown()

	rows := harness.mustQuery(context.Background(), "show tables")
	defer rows.Close()
	require.NoError(t, rows.Err())

	output := make([]string, 0)
	for rows.Next() {
		var table string

		err := rows.Scan(&table)
		assert.NoError(t, err, "rows.Scan()")

		output = append(output, table)
	}

	assert.Equal(t, 1, len(output), "query output")
}

type dummyRow struct {
	NullValue     *struct{}       `json:"nullValue"`
	SmallintType  int             `json:"smallintType"`
	IntType       int             `json:"intType"`
	BigintType    int             `json:"bigintType"`
	BooleanType   bool            `json:"booleanType"`
	FloatType     float32         `json:"floatType"`
	DoubleType    float64         `json:"doubleType"`
	StringType    string          `json:"stringType"`
	TimestampType athenaTimestamp `json:"timestampType"`
	DateType      athenaDate      `json:"dateType"`
	DecimalType   float64         `json:"decimalType"`
}

type athenaHarness struct {
	t    *testing.T
	db   *sql.DB
	sess *session.Session

	table string
}

func setup(t *testing.T) *athenaHarness {
	var acfg []*aws.Config
	acfg = append(acfg, &aws.Config{
		Region: aws.String(AwsRegion),
	})
	sess, err := session.NewSession(acfg...)
	if err != nil {
		require.NoError(t, err)
	}
	harness := athenaHarness{t: t, sess: sess}

	connStr := fmt.Sprintf("db=%s&output_location=s3://%s&region=%s", AthenaDatabase, S3Bucket, AwsRegion)
	if AutoOutputLocation {
		connStr = fmt.Sprintf("db=%s&region=%s&workgroup=%s", AthenaDatabase, AwsRegion, WorkGroup)
	}

	harness.db, err = sql.Open("athena", connStr)
	require.NoError(t, err)

	harness.setupTable()

	return &harness
}

func (a *athenaHarness) setupTable() {
	// tables cannot start with numbers or contain dashes
	id := uuid.NewV4()
	a.table = "t_" + strings.Replace(id.String(), "-", "_", -1)
	a.mustExec(`CREATE EXTERNAL TABLE %[1]s (
	nullValue string,
	smallintType smallint,
	intType int,
	bigintType bigint,
	booleanType boolean,
	floatType float,
	doubleType double,
	stringType string,
	timestampType timestamp,
	dateType date,
	decimalType decimal(11, 5)
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
	'serialization.format' = '1'
) LOCATION 's3://%[2]s/%[1]s/';`, a.table, S3Bucket)
}

func (a *athenaHarness) teardown() {
	a.mustExec("drop table %s", a.table)
}

func (a *athenaHarness) mustExec(sql string, args ...interface{}) {
	query := fmt.Sprintf(sql, args...)
	_, err := a.db.ExecContext(context.TODO(), query)
	require.NoError(a.t, err, query)
}

func (a *athenaHarness) mustQuery(ctx context.Context, sql string, args ...interface{}) *sql.Rows {
	query := fmt.Sprintf(sql, args...)
	rows, err := a.db.QueryContext(ctx, query)
	require.NoError(a.t, err, query)
	return rows
}

func (a *athenaHarness) uploadData(rows []dummyRow) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, row := range rows {
		err := enc.Encode(row)
		require.NoError(a.t, err)
	}

	uploader := s3manager.NewUploader(a.sess)

	_, err := uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(S3Bucket),
		Key:    aws.String(fmt.Sprintf("%s/fixture.json", a.table)),
		Body:   bytes.NewReader(buf.Bytes()),
	})
	require.NoError(a.t, err)
}

type athenaTimestamp time.Time

func (t athenaTimestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

func (t athenaTimestamp) String() string {
	return time.Time(t).Format(TimestampLayout)
}

func (t athenaTimestamp) Equal(t2 athenaTimestamp) bool {
	return time.Time(t).Equal(time.Time(t2))
}

type athenaDate time.Time

func (t athenaDate) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.String())
}

func (t athenaDate) String() string {
	return time.Time(t).Format(DateLayout)
}

func (t athenaDate) Equal(t2 athenaDate) bool {
	return time.Time(t).Equal(time.Time(t2))
}
