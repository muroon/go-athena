package athena

import (
	"database/sql/driver"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/pkg/errors"
)

const (
	// TimestampLayout is the Go time layout string for an Athena `timestamp`.
	TimestampLayout             = "2006-01-02 15:04:05.999"
	TimestampWithTimeZoneLayout = "2006-01-02 15:04:05.999 MST"
	DateLayout                  = "2006-01-02"
)

const nullStringResultModeGzipDL string = "\\N"

func convertRow(columns []types.ColumnInfo, data []types.Datum, dest []driver.Value) error {
	for i := range data {
		if i >= len(dest) {
			return errors.New("destination slice is too short")
		}

		if data[i].VarCharValue == nil {
			dest[i] = nil
			continue
		}

		colType := ""
		if i < len(columns) && columns[i].Type != nil {
			colType = *columns[i].Type
		}

		v, err := convertValueByColumnType(*data[i].VarCharValue, colType)
		if err != nil {
			return errors.Wrapf(err, "failed to convert value at index %d", i)
		}
		dest[i] = v
	}
	return nil
}

func convertRowFromCsv(columns []types.ColumnInfo, fields []downloadField, dest []driver.Value) error {
	for i := range fields {
		if i >= len(dest) {
			return errors.New("destination slice is too short")
		}

		if fields[i].isNil {
			dest[i] = nil
			continue
		}

		colType := ""
		if i < len(columns) && columns[i].Type != nil {
			colType = *columns[i].Type
		}

		v, err := convertValueByColumnType(fields[i].val, colType)
		if err != nil {
			return errors.Wrapf(err, "failed to convert CSV value at index %d", i)
		}
		dest[i] = v
	}
	return nil
}

func convertRowFromTableInfo(columns []types.Column, raw []string, dest []driver.Value) error {
	for i := range raw {
		if i >= len(dest) {
			return errors.New("destination slice is too short")
		}

		colType := ""
		if i < len(columns) && columns[i].Type != nil {
			colType = *columns[i].Type
		}

		v, err := convertValueByColumnType(raw[i], colType)
		if err != nil {
			return errors.Wrapf(err, "failed to convert table value at index %d", i)
		}
		dest[i] = v
	}
	return nil
}

func convertValueByColumnType(s string, columnType string) (interface{}, error) {
	switch columnType {
	case "tinyint", "smallint", "integer", "int":
		i, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse integer value: %s", s)
		}
		return int32(i), nil
	case "bigint":
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse bigint value: %s", s)
		}
		return i, nil
	case "double", "float":
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse float value: %s", s)
		}
		return f, nil
	case "boolean":
		b, err := strconv.ParseBool(s)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse boolean value: %s", s)
		}
		return b, nil
	case "timestamp":
		t, err := time.Parse(TimestampLayout, s)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse timestamp value: %s", s)
		}
		return t, nil
	case "timestamp with time zone":
		t, err := time.Parse(TimestampWithTimeZoneLayout, s)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse timestamp value: %s", s)
		}
		return t, nil
	case "date":
		t, err := time.Parse(DateLayout, s)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse date value: %s", s)
		}
		return t, nil
	default:
		return s, nil
	}
}
