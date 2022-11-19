package main

import (
	"fmt"
	"time"

	"github.com/zx06/text-classify/handler"
)

func main() {
	t0:=time.Now()
	err := handler.Handler("data/records", "data/results", ".txt", 1000)
	if err != nil {
		panic(err)
	}
	fmt.Println("cost: "+time.Since(t0).String())
}