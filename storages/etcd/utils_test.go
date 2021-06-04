package etcd

import (
	"testing"
)

func Test_abs(t *testing.T) {
	tests := []struct {
		name string
		have int
		want int
	}{
		{
			name: "positive",
			have: 1,
			want: 1,
		},
		{
			name: "negative",
			have: -1,
			want: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := abs(tt.have); got != tt.want {
				t.Errorf("abs() = %v, want %v", got, tt.want)
			}
		})
	}
}
