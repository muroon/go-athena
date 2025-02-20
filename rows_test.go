package athena

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"math/rand"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
	"github.com/stretchr/testify/assert"
)

var dummyError = errors.New("dummy error")

type genQueryResultsOutputByToken func(token *string) (*athena.GetQueryResultsOutput, error)

var queryToResultsGenMap = map[string]genQueryResultsOutputByToken{
	"select":         dummySelectQueryResponse,
	"select_zero":    dummySelectZeroQueryResponse,
	"show":           dummyShowResponse,
	"iteration_fail": dummyFailedIterationResponse,
}

func genColumnInfo(column string) types.ColumnInfo {
	caseSensitive := true
	catalogName := "hive"
	nullable := types.ColumnNullableUnknown
	precision := int32(2147483647)
	scale := int32(0)
	schemaName := ""
	tableName := ""
	columnType := "varchar"

	return types.ColumnInfo{
		CaseSensitive: caseSensitive,
		CatalogName:   &catalogName,
		Nullable:      nullable,
		Precision:     precision,
		Scale:         scale,
		SchemaName:    &schemaName,
		TableName:     &tableName,
		Type:          &columnType,
		Label:         &column,
		Name:          &column,
	}
}

func randomString() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	s := make([]byte, 10)
	for i := 0; i < len(s); i++ {
		s[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return string(s)
}

func genRow(isHeader bool, columns []types.ColumnInfo) types.Row {
	var data []types.Datum
	for i := 0; i < len(columns); i++ {
		if isHeader {
			data = append(data, types.Datum{
				VarCharValue: columns[i].Name,
			})
		} else {
			s := randomString()
			data = append(data, types.Datum{
				VarCharValue: &s,
			})
		}
	}
	return types.Row{
		Data: data,
	}
}

func dummySelectQueryResponse(token *string) (*athena.GetQueryResultsOutput, error) {
	tokenStr := ""
	if token != nil {
		tokenStr = *token
	}
	switch tokenStr {
	case "":
		nextToken := "page_1"
		columns := []types.ColumnInfo{
			genColumnInfo("first_name"),
			genColumnInfo("last_name"),
		}
		return &athena.GetQueryResultsOutput{
			NextToken: &nextToken,
			ResultSet: &types.ResultSet{
				ResultSetMetadata: &types.ResultSetMetadata{
					ColumnInfo: columns,
				},
				Rows: []types.Row{
					genRow(true, columns),
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
				},
			},
		}, nil
	case "page_1":
		columns := []types.ColumnInfo{
			genColumnInfo("first_name"),
			genColumnInfo("last_name"),
		}
		return &athena.GetQueryResultsOutput{
			ResultSet: &types.ResultSet{
				ResultSetMetadata: &types.ResultSetMetadata{
					ColumnInfo: columns,
				},
				Rows: []types.Row{
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
				},
			},
		}, nil
	default:
		return nil, dummyError
	}
}

func dummySelectZeroQueryResponse(token *string) (*athena.GetQueryResultsOutput, error) {
	columns := []types.ColumnInfo{
		genColumnInfo("first_name"),
		genColumnInfo("last_name"),
	}
	return &athena.GetQueryResultsOutput{
		ResultSet: &types.ResultSet{
			ResultSetMetadata: &types.ResultSetMetadata{
				ColumnInfo: columns,
			},
			Rows: []types.Row{
				genRow(true, columns),
			},
		},
	}, nil
}

func dummyShowResponse(_ *string) (*athena.GetQueryResultsOutput, error) {
	columns := []types.ColumnInfo{
		genColumnInfo("partition"),
	}
	return &athena.GetQueryResultsOutput{
		ResultSet: &types.ResultSet{
			ResultSetMetadata: &types.ResultSetMetadata{
				ColumnInfo: columns,
			},
			Rows: []types.Row{
				genRow(false, columns),
				genRow(false, columns),
			},
		},
	}, nil
}

func dummyFailedIterationResponse(token *string) (*athena.GetQueryResultsOutput, error) {
	tokenStr := ""
	if token != nil {
		tokenStr = *token
	}
	switch tokenStr {
	case "":
		nextToken := "page_1"
		columns := []types.ColumnInfo{
			genColumnInfo("first_name"),
			genColumnInfo("last_name"),
		}
		return &athena.GetQueryResultsOutput{
			NextToken: &nextToken,
			ResultSet: &types.ResultSet{
				ResultSetMetadata: &types.ResultSetMetadata{
					ColumnInfo: columns,
				},
				Rows: []types.Row{
					genRow(true, columns),
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
					genRow(false, columns),
				},
			},
		}, nil
	default:
		return nil, dummyError
	}
}

type mockAthenaClient struct {
	*athena.Client
}

func NewMockAthenaClient() *athena.Client {
	return &athena.Client{}
}

func (m *mockAthenaClient) GetQueryResults(ctx context.Context, params *athena.GetQueryResultsInput, optFns ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error) {
	return queryToResultsGenMap[*params.QueryExecutionId](params.NextToken)
}

func castToValue(dest ...driver.Value) []driver.Value {
	return dest
}

func TestRows_Next(t *testing.T) {
	tests := []struct {
		desc                string
		queryID             string
		skipHeader          bool
		expectedResultsSize int
		expectedError       error
	}{
		{
			desc:                "show query, no header, 2 rows, no error",
			queryID:             "show",
			skipHeader:          false,
			expectedResultsSize: 2,
			expectedError:       nil,
		},
		{
			desc:                "select query, header, 0 rows, no error",
			queryID:             "select_zero",
			skipHeader:          true,
			expectedResultsSize: 0,
			expectedError:       nil,
		},
		{
			desc:                "select query, header, multipage, 9 rows, no error",
			queryID:             "select",
			skipHeader:          true,
			expectedResultsSize: 9,
			expectedError:       nil,
		},
		{
			desc:          "failed during calling next",
			queryID:       "iteration_fail",
			skipHeader:    true,
			expectedError: dummyError,
		},
	}
	for _, test := range tests {
		r, _ := newRows(rowsConfig{
			Athena:     NewMockAthenaClient(),
			QueryID:    test.queryID,
			SkipHeader: test.skipHeader,
		})

		var firstName, lastName string
		cnt := 0
		for {
			err := r.Next(castToValue(&firstName, &lastName))
			if err != nil {
				if err != io.EOF {
					assert.Equal(t, test.expectedError, err)
				}
				break
			}
			cnt++
		}
		if test.expectedError == nil {
			assert.Equal(t, test.expectedResultsSize, cnt)
		}
	}
}

func Test_getRecordsForDL(t *testing.T) {

	tests := []struct {
		name    string
		param   string
		want    [][]downloadField
		wantErr bool
	}{
		{
			name:  "test",
			param: ",\"1\"\n\"\",\"9\"\n\"hoge, hoge\",\"10\"",
			want: [][]downloadField{
				{
					{
						isNil: true,
					},
					{
						val: "1",
					},
				},
				{
					{
						isNil: false,
						val:   "",
					},
					{
						val: "9",
					},
				},
				{
					{
						isNil: false,
						val:   "hoge, hoge",
					},
					{
						val: "10",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getRecordsForDL(strings.NewReader(tt.param))
			if (err != nil) != tt.wantErr {
				t.Errorf("getRecordsForDL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			for i, dfs := range got {
				for j, df := range dfs {
					want := tt.want[i][j]
					if want != df {
						t.Errorf("getRecordsForDL() expecte:%v, actual:%v", want, df)
					}
				}
			}
		})
	}
}
