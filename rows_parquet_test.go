package athena

import (
	"bufio"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
)

func parseTestInput(input string) ([][]string, error) {
	records := make([][]string, 0)

	scanner := bufio.NewScanner(strings.NewReader(input))

	for scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, err
		}
		b := scanner.Bytes()
		field := ""
		record := make([]string, 0)
		for {
			if len(b) == 0 {
				if field == "" || field == nullStringParquet || field == "null" {
					record = append(record, nullStringResultModeGzipDL)
				} else {
					record = append(record, field)
				}
				break
			}
			
			r, width := utf8.DecodeRune(b)
			if r == '\001' {
				if field == "" || field == nullStringParquet || field == "null" {
					record = append(record, nullStringResultModeGzipDL)
				} else {
					record = append(record, field)
				}
				field = ""
			} else {
				field += string(r)
			}
			if width >= len(b) {
				if field == "" || field == nullStringParquet || field == "null" {
					record = append(record, nullStringResultModeGzipDL)
				} else {
					record = append(record, field)
				}
				break
			}
			b = b[width:]
		}

		records = append(records, record)
	}

	return records, nil
}

func Test_getRecordsFromParquet(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected [][]string
	}{
		{
			name:     "empty input",
			input:    "",
			expected: [][]string{},
		},
		{
			name:     "single record with no nulls",
			input:    "value1\001value2\001value3",
			expected: [][]string{{"value1", "value2", "value3"}},
		},
		{
			name:     "single record with empty values",
			input:    "value1\001\001value3",
			expected: [][]string{{"value1", nullStringResultModeGzipDL, "value3"}},
		},
		{
			name:     "single record with null values",
			input:    "value1\001null\001value3",
			expected: [][]string{{"value1", nullStringResultModeGzipDL, "value3"}},
		},
		{
			name:     "multiple records with nulls",
			input:    "value1\001null\001value3\nvalue4\001value5\001null",
			expected: [][]string{
				{"value1", nullStringResultModeGzipDL, "value3"},
				{"value4", "value5", nullStringResultModeGzipDL},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			records, err := parseTestInput(tt.input)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, records)
		})
	}
}
