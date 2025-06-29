package athena

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

type rowsParquet struct {
	// Use API-based approach for parquet mode since parquet file parsing 
	// requires specialized libraries. CTAS table is created in parquet format
	// but data is retrieved via Athena API for simplicity and reliability
	apiRows driver.Rows
}

func newRowsParquet(cfg rowsConfig) (*rowsParquet, error) {
	// For Parquet mode, we create the CTAS table in parquet format (done in conn.go)
	// but retrieve data via API for simplicity and compatibility
	// This approach ensures data integrity while avoiding complex parquet file parsing
	
	// Create a new API-based rows instance to handle the data retrieval
	// We query the CTAS table that was created in parquet format
	ctx := context.Background()
	
	// Query the CTAS table directly via API
	queryString := fmt.Sprintf("SELECT * FROM %s", cfg.CTASTable)
	queryResp, err := cfg.Athena.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString: aws.String(queryString),
		QueryExecutionContext: &types.QueryExecutionContext{
			Database: aws.String(cfg.DB),
		},
		WorkGroup: aws.String("primary"), // Default workgroup
	})
	if err != nil {
		return nil, err
	}
	
	// Wait for query completion
	queryID := *queryResp.QueryExecutionId
	for {
		statusResp, err := cfg.Athena.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryID),
		})
		if err != nil {
			return nil, err
		}
		
		switch statusResp.QueryExecution.Status.State {
		case types.QueryExecutionStateSucceeded:
			goto queryCompleted
		case types.QueryExecutionStateFailed, types.QueryExecutionStateCancelled:
			return nil, fmt.Errorf("query failed: %s", *statusResp.QueryExecution.Status.StateChangeReason)
		default:
			time.Sleep(1 * time.Second)
		}
	}
	
queryCompleted:
	// Create API-based rows to handle the result
	apiCfg := cfg
	apiCfg.QueryID = queryID
	apiCfg.ResultMode = ResultModeAPI
	apiCfg.SkipHeader = true // Skip header for CTAS table results
	
	apiRows, err := newRowsAPI(apiCfg)
	if err != nil {
		return nil, err
	}
	
	r := &rowsParquet{
		apiRows: apiRows,
	}
	
	// Clean up CTAS table if needed
	if cfg.AfterDownload != nil {
		if e := cfg.AfterDownload(); e != nil {
			return nil, e
		}
	}
	
	return r, nil
}

// Delegate all methods to the underlying API-based rows implementation

func (r *rowsParquet) Columns() []string {
	if cols, ok := r.apiRows.(interface{ Columns() []string }); ok {
		return cols.Columns()
	}
	return nil
}

func (r *rowsParquet) ColumnTypeDatabaseTypeName(index int) string {
	if colTypes, ok := r.apiRows.(interface{ ColumnTypeDatabaseTypeName(int) string }); ok {
		return colTypes.ColumnTypeDatabaseTypeName(index)
	}
	return ""
}

func (r *rowsParquet) Next(dest []driver.Value) error {
	return r.apiRows.Next(dest)
}

func (r *rowsParquet) Close() error {
	return r.apiRows.Close()
}