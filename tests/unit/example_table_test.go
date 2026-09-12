// Package example_test demonstrates the table-driven test pattern used
// throughout SamanvayDB. Copy this template when writing new tests.
package unit

import (
	"fmt"
	"testing"
)

// add is a trivial function used to demonstrate the pattern.
func add(a, b int) int { return a + b }

// TestAdd_TableDriven shows the canonical table-driven test structure.
// Each test case is a struct with inputs, expected outputs, and a descriptive name.
func TestAdd_TableDriven(t *testing.T) {
	tests := []struct {
		name string
		a, b int
		want int
	}{
		{name: "positive_numbers", a: 2, b: 3, want: 5},
		{name: "zero_and_positive", a: 0, b: 7, want: 7},
		{name: "negative_numbers", a: -1, b: -2, want: -3},
		{name: "mixed_sign", a: -5, b: 10, want: 5},
		{name: "both_zero", a: 0, b: 0, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := add(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("add(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// TestAdd_ErrorCases demonstrates testing error conditions with table-driven tests.
// In real code, the function under test would return (result, error).
func TestAdd_ErrorCases(t *testing.T) {
	type result struct {
		value int
		err   error
	}

	safeDiv := func(a, b int) (int, error) {
		if b == 0 {
			return 0, fmt.Errorf("division by zero")
		}
		return a / b, nil
	}

	tests := []struct {
		name    string
		a, b    int
		want    int
		wantErr bool
	}{
		{name: "normal_division", a: 10, b: 2, want: 5, wantErr: false},
		{name: "divide_by_zero", a: 10, b: 0, want: 0, wantErr: true},
		{name: "negative_divisor", a: 10, b: -2, want: -5, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := safeDiv(tt.a, tt.b)
			if (err != nil) != tt.wantErr {
				t.Fatalf("safeDiv(%d, %d) error = %v, wantErr %v", tt.a, tt.b, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("safeDiv(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}
