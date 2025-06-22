package athena

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"time"

	"database/sql"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

const (
	// TimestampLayout is the Go time layout string for an Athena `timestamp`.
	TimestampLayout             = "2006-01-02 15:04:05.999"
	TimestampWithTimeZoneLayout = "2006-01-02 15:04:05.999 MST"
	DateLayout                  = "2006-01-02"
)

const nullStringResultModeGzipDL string = "\\N"
const nullStringResultModeParquetDL string = "\\N"

func convertRow(columns []types.ColumnInfo, in []types.Datum, ret []driver.Value) error {
	for i, val := range in {
		coerced, err := convertValue(*columns[i].Type, val.VarCharValue)
		if err != nil {
			return err
		}

		ret[i] = coerced
	}

	return nil
}

func convertRowFromTableInfo(columns []types.Column, in []string, ret []driver.Value) error {
	for i, val := range in {
		var coerced interface{}
		var err error
		if val == nullStringResultModeGzipDL || val == nullStringResultModeParquetDL {
			var nullVal *string
			coerced, err = convertValue(*columns[i].Type, nullVal)
		} else {
			coerced, err = convertValue(*columns[i].Type, &val)
		}
		if err != nil {
			return err
		}

		ret[i] = coerced
	}

	return nil
}

func convertRowFromCsv(columns []types.ColumnInfo, in []downloadField, ret []driver.Value) error {
	for i, df := range in {
		var coerced interface{}
		var err error
		if df.isNil {
			var nullVal *string
			coerced, err = convertValue(*columns[i].Type, nullVal)
		} else {
			coerced, err = convertValue(*columns[i].Type, &df.val)
		}
		if err != nil {
			return err
		}

		ret[i] = coerced
	}

	return nil
}

func convertValue(athenaType string, rawValue *string) (interface{}, error) {
	if rawValue == nil || *rawValue == nullStringResultModeGzipDL || *rawValue == nullStringResultModeParquetDL {
		switch athenaType {
		case "struct", "string", "varchar":
			return sql.NullString{Valid: false}, nil
		case "smallint", "integer", "int", "bigint":
			return sql.NullInt64{Valid: false}, nil
		case "boolean":
			return sql.NullBool{Valid: false}, nil
		case "float", "double", "decimal":
			return sql.NullFloat64{Valid: false}, nil
		case "timestamp", "timestamp with time zone", "date":
			return time.Time{}, nil
		default:
			return nil, nil
		}
	}

	if len(athenaType) > 7 && athenaType[:7] == "decimal" {
		athenaType = "decimal"
	}

	val := *rawValue
	switch athenaType {
	case "struct", "string", "varchar":
		return sql.NullString{String: val, Valid: true}, nil
	case "smallint", "integer", "int", "bigint":
		i, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, err
		}
		return sql.NullInt64{Int64: i, Valid: true}, nil
	case "boolean":
		b, err := strconv.ParseBool(val)
		if err != nil {
			return nil, err
		}
		return sql.NullBool{Bool: b, Valid: true}, nil
	case "float", "double", "decimal":
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, err
		}
		return sql.NullFloat64{Float64: f, Valid: true}, nil
	case "timestamp", "timestamp with time zone":
		return time.Parse(TimestampLayout, val)
	case "date":
		return time.Parse(DateLayout, val)
	default:
		return nil, fmt.Errorf("unsupported type: %s", athenaType)
	}
}
