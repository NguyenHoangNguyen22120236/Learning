package main

import (
	"fmt"
	//"slices"
)

func add(s []int) {
    s = append(s, 40)
}

func main() {
    numbers := []int{10, 20, 30}

    add(numbers)

    fmt.Println(numbers)
}