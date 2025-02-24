package athena

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type stmtAthena struct {
	prepareKey    string
	numInput      int
	ctasTable     string
	afterDownload func() error
	conn          *conn
	resultMode    ResultMode
}

func (s *stmtAthena) Close() error {
	return nil
}

func (s *stmtAthena) NumInput() int {
	return s.numInput
}

func (s *stmtAthena) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), convertValues(args))
}

func (s *stmtAthena) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	query := fmt.Sprintf("EXECUTE %s USING %s", s.prepareKey, formatArgs(args))
	return s.conn.ExecContext(ctx, query, nil)
}

func (s *stmtAthena) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), convertValues(args))
}

func (s *stmtAthena) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	query := fmt.Sprintf("EXECUTE %s USING %s", s.prepareKey, formatArgs(args))
	return s.conn.QueryContext(ctx, query, nil)
}

// Helper functions
func convertValues(args []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(args))
	for i, v := range args {
		namedValues[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return namedValues
}

func formatArgs(args []driver.NamedValue) string {
	if len(args) == 0 {
		return ""
	}

	values := make([]string, len(args))
	for i, arg := range args {
		switch v := arg.Value.(type) {
		case string:
			values[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
		case nil:
			values[i] = "NULL"
		default:
			values[i] = fmt.Sprint(v)
		}
	}
	return strings.Join(values, ", ")
}

func convertNamedValues(named []driver.NamedValue) []interface{} {
	args := make([]interface{}, len(named))
	for i, n := range named {
		args[i] = n.Value
	}
	return args
}

func join(args []string) string {
	return strings.Join(args, ", ")
}

func serial(ctx context.Context, v interface{}) (string, error) {
	if v == nil {
		return "NULL", nil
	}

	switch v := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", strings.Replace(v, "'", "''", -1)), nil
	case bool:
		return fmt.Sprintf("%t", v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case time.Time:
		return fmt.Sprintf("TIMESTAMP '%s'", v.Format("2006-01-02 15:04:05.999")), nil
	}

	return "", fmt.Errorf("unsupported type: %T", v)
}
