package main

import (
	"flag"
	"fmt"
)

func main() {
	fmt.Println(cmd.Name())
	cmd.Visit(func(f *flag.Flag) {
		fmt.Printf("option %s, value is %s\n", f.Name, f.Value)
	})

}
