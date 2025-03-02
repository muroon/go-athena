package athena

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/athena"
)

// AthenaAPI is an interface that wraps the necessary methods from the AWS SDK v2 Athena client
type AthenaAPI interface {
	StartQueryExecution(ctx context.Context, params *athena.StartQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StartQueryExecutionOutput, error)
	GetQueryExecution(ctx context.Context, params *athena.GetQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.GetQueryExecutionOutput, error)
	GetQueryResults(ctx context.Context, params *athena.GetQueryResultsInput, optFns ...func(*athena.Options)) (*athena.GetQueryResultsOutput, error)
	StopQueryExecution(ctx context.Context, params *athena.StopQueryExecutionInput, optFns ...func(*athena.Options)) (*athena.StopQueryExecutionOutput, error)
	GetWorkGroup(ctx context.Context, params *athena.GetWorkGroupInput, optFns ...func(*athena.Options)) (*athena.GetWorkGroupOutput, error)
}

// Ensure that the v2 Athena client implements our interface
var _ AthenaAPI = (*athena.Client)(nil)
