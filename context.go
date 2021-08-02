package athena

import "context"

const contextPrefix string = "go-athena"

/*
 * Result Mode
 */

const resultModeContextKey string = "result_mode_key"

// ResultModeContextKey context key of setting result mode
var ResultModeContextKey string = contextPrefix + resultModeContextKey

// SetAPIMode set APIMode to ResultMode from context
func SetAPIMode(ctx context.Context) context.Context {
	return context.WithValue(ctx, ResultModeContextKey, ResultModeAPI)
}

// SetDLMode set DownloadMode to ResultMode from context
func SetDLMode(ctx context.Context) context.Context {
	return context.WithValue(ctx, ResultModeContextKey, ResultModeDL)
}

// SetGzipDLMode set CTASMode to ResultMode from context
func SetGzipDLMode(ctx context.Context) context.Context {
	return context.WithValue(ctx, ResultModeContextKey, ResultModeGzipDL)
}

func getResultMode(ctx context.Context) (ResultMode, bool) {
	val, ok := ctx.Value(ResultModeContextKey).(ResultMode)
	return val, ok
}

/*
 * timeout
 */

const timeoutContextKey string = "timeout_key"

// TimeoutContextKey context key of setting timeout
var TimeoutContextKey string = contextPrefix + timeoutContextKey

// SetTimeout set timeout from context
func SetTimeout(ctx context.Context, timeout uint) context.Context {
	return context.WithValue(ctx, TimeoutContextKey, timeout)
}

func getTimeout(ctx context.Context) (uint, bool) {
	val, ok := ctx.Value(TimeoutContextKey).(uint)
	return val, ok
}

/*
 * catalog
 */

const catalogContextKey string = "catalog_key"

// CatalogContextKey context key of setting catalog
var CatalogContextKey string = contextPrefix + catalogContextKey

// SetCatalog set catalog from context
func SetCatalog(ctx context.Context, catalog string) context.Context {
	return context.WithValue(ctx, CatalogContextKey, catalog)
}

func getCatalog(ctx context.Context) (string, bool) {
	val, ok := ctx.Value(CatalogContextKey).(string)
	return val, ok
}

/*
 * force using string type with numeric string
 */
const forceNumericContextKey string = "force_numeric_string_key"

// ForceNumericStringContextKey context key of force numeric string
var ForceNumericStringContextKey = contextPrefix + forceNumericContextKey

func SetForceNumericString(ctx context.Context) context.Context {
	return context.WithValue(ctx, ForceNumericStringContextKey, true)
}

func getForNumericString(ctx context.Context) bool {
	val, ok := ctx.Value(ForceNumericStringContextKey).(bool)
	return val && ok
}
