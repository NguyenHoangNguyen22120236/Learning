package main

import (
	"fmt"
)

type AppError struct {
    Message string
}

func (e *AppError) Error() string {
    return e.Message
}

func process() error {
    var err *AppError = nil

    return err
}

func main(){
	err := process()
	_int := 12

	fmt.Printf("type %T\n", _int)

	fmt.Printf("type %T\n", err)

	fmt.Printf("type %T\n", err)

	fmt.Println(err == nil)
}