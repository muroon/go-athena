package athena

import (
	"database/sql/driver"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

const (
	// TimestampLayout is the Go time layout string for an Athena `timestamp`.
	TimestampLayout             = "2006-01-02 15:04:05.999"
	TimestampWithTimeZoneLayout = "2006-01-02 15:04:05.999 MST"
	DateLayout                  = "2006-01-02"
	nullStringResultModeGzipDL  = "\\N"
)

type ColumnType string

const (
	ColumnTypeVarchar   ColumnType = "varchar"
	ColumnTypeInteger   ColumnType = "integer"
	ColumnTypeBoolean   ColumnType = "boolean"
	ColumnTypeDouble    ColumnType = "double"
	ColumnTypeTimestamp ColumnType = "timestamp"
	// Add other types as needed
)

func NewColumnType(typeStr string) *ColumnType {
	ct := ColumnType(typeStr)
	return &ct
}

func (ct *ColumnType) DatabaseTypeName() string {
	return string(*ct)
}

func (ct *ColumnType) ConvertValue(val string) (interface{}, error) {
	switch *ct {
	case ColumnTypeVarchar:
		return val, nil
	// Add other type conversions as needed
	default:
		return val, nil
	}
}

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
		if val == nullStringResultModeGzipDL {
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
	if rawValue == nil {
		return nil, nil
	}

	if len(athenaType) > 7 && athenaType[:7] == "decimal" {
		athenaType = "decimal"
	}

	val := *rawValue
	switch athenaType {
	case "smallint":
		return strconv.ParseInt(val, 10, 16)
	case "integer", "int":
		return strconv.ParseInt(val, 10, 32)
	case "bigint":
		return strconv.ParseInt(val, 10, 64)
	case "boolean":
		switch val {
		case "true":
			return true, nil
		case "false":
			return false, nil
		}
		return nil, fmt.Errorf("cannot parse '%s' as boolean", val)
	case "float":
		return strconv.ParseFloat(val, 32)
	case "double", "decimal":
		return strconv.ParseFloat(val, 64)
	case "varchar", "string":
		return val, nil
	case "timestamp":
		return time.Parse(TimestampLayout, val)
	case "timestamp with time zone":
		return time.Parse(TimestampWithTimeZoneLayout, val)
	case "date":
		return time.Parse(DateLayout, val)
	default:
		panic(fmt.Errorf("unknown type `%s` with value %s", athenaType, val))
	}
}
