package main

import (
	"fmt"
	"os"

	"github.com/bmeg/benchtop/cmdline/benchtop/cmds"
)

func main() {
	if err := cmds.RootCmd.Execute(); err != nil {
		fmt.Println("Error:", err.Error())
		os.Exit(1)
	}
}
