package main

import (
	"fmt"
	"maps"
	"slices"
)

// CountRunes counts Unicode code points, including spaces and punctuation.
func CountRunes(s string) map[rune]int {
	counts := make(map[rune]int)
	for _, r := range s {
		counts[r]++
	}
	return counts
}

func main() {
	for _, s := range []string{"Hello", "Xin chào", "你好你好"} {
		counts := CountRunes(s)
		fmt.Printf("%q:\n", s)
		// Sort keys by Unicode code point for deterministic output.
		for _, r := range slices.Sorted(maps.Keys(counts)) {
			fmt.Printf("  %q: %d\n", r, counts[r])
		}
	}
}
