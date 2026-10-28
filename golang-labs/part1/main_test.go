package main

import (
	"maps"
	"testing"
)

func TestCountRunes(t *testing.T) {
	tests := []struct {
		input string
		want  map[rune]int
	}{
		{"Hello", map[rune]int{'H': 1, 'e': 1, 'l': 2, 'o': 1}},
		{"Xin chào", map[rune]int{'X': 1, 'i': 1, 'n': 1, ' ': 1, 'c': 1, 'h': 1, 'à': 1, 'o': 1}},
		{"你好你好", map[rune]int{'你': 2, '好': 2}},
		{"", map[rune]int{}},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := CountRunes(tt.input); !maps.Equal(got, tt.want) {
				t.Errorf("CountRunes(%q) = %v; want %v", tt.input, got, tt.want)
			}
		})
	}
}
