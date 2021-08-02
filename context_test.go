package athena

import (
	"context"
	"testing"
)

func Test_getForNumericString(t *testing.T) {
	tests := []struct {
		name string
		ctx  context.Context
		want bool
	}{
		{
			name: "Default",
			ctx:  context.Background(),
			want: false,
		},
		{
			name: "SetForceNumericString",
			ctx:  SetForceNumericString(context.Background()),
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getForNumericString(tt.ctx); got != tt.want {
				t.Errorf("getForNumericString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getCatalog(t *testing.T) {
	tests := []struct {
		name  string
		ctx   context.Context
		want  string
		want1 bool
	}{
		{
			name:  "Default",
			ctx:   context.Background(),
			want:  "",
			want1: false,
		},
		{
			name:  "SetCatalog",
			ctx:   SetCatalog(context.Background(), "test_catalog"),
			want:  "test_catalog",
			want1: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := getCatalog(tt.ctx)
			if got != tt.want {
				t.Errorf("getCatalog() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("getCatalog() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_getTimeout(t *testing.T) {
	tests := []struct {
		name  string
		ctx   context.Context
		want  uint
		want1 bool
	}{
		{
			name:  "Default",
			ctx:   context.Background(),
			want:  0,
			want1: false,
		},
		{
			name:  "SetTimeout",
			ctx:   SetTimeout(context.Background(), 100),
			want:  100,
			want1: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := getTimeout(tt.ctx)
			if got != tt.want {
				t.Errorf("getTimeout() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("getTimeout() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
