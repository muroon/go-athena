package athena

import "context"

// contextKey is a type for context keys to ensure type safety
type contextKey string

const contextPrefix = "go-athena"

// Context keys
const (
	resultModeKey contextKey = contextKey(contextPrefix + "result_mode_key")
	timeoutKey    contextKey = contextKey(contextPrefix + "timeout_key")
	catalogKey    contextKey = contextKey(contextPrefix + "catalog_key")
)

// ResultModeContextKey is deprecated, use resultModeKey instead
var ResultModeContextKey = string(resultModeKey)

// TimeoutContextKey is deprecated, use timeoutKey instead
var TimeoutContextKey = string(timeoutKey)

// CatalogContextKey is deprecated, use catalogKey instead
var CatalogContextKey = string(catalogKey)

// contextValue safely retrieves a typed value from context
func contextValue[T any](ctx context.Context, key contextKey) (T, bool) {
	v := ctx.Value(key)
	if v == nil {
		var zero T
		return zero, false
	}
	val, ok := v.(T)
	return val, ok
}

// SetResultMode sets the ResultMode in context
func SetResultMode(ctx context.Context, mode ResultMode) context.Context {
	return context.WithValue(ctx, resultModeKey, mode)
}

// SetAPIMode sets APIMode to ResultMode in context
func SetAPIMode(ctx context.Context) context.Context {
	return SetResultMode(ctx, ResultModeAPI)
}

// SetDLMode sets DownloadMode to ResultMode in context
func SetDLMode(ctx context.Context) context.Context {
	return SetResultMode(ctx, ResultModeDL)
}

// SetGzipDLMode sets GzipDLMode to ResultMode in context
func SetGzipDLMode(ctx context.Context) context.Context {
	return SetResultMode(ctx, ResultModeGzipDL)
}

func getResultMode(ctx context.Context) (ResultMode, bool) {
	return contextValue[ResultMode](ctx, resultModeKey)
}

// SetTimeout sets timeout in context
func SetTimeout(ctx context.Context, timeout uint) context.Context {
	return context.WithValue(ctx, timeoutKey, timeout)
}

func getTimeout(ctx context.Context) (uint, bool) {
	return contextValue[uint](ctx, timeoutKey)
}

// SetCatalog sets catalog in context
func SetCatalog(ctx context.Context, catalog string) context.Context {
	return context.WithValue(ctx, catalogKey, catalog)
}

func getCatalog(ctx context.Context) (string, bool) {
	return contextValue[string](ctx, catalogKey)
}
