package main

import (
	"flag"
	"fmt"
	"os"
)

type Flags struct {
	*flag.FlagSet
	cmdUsage string
}

var cmd *Flags

func init() {
	getCmd := &Flags{
		FlagSet:  flag.NewFlagSet("get", flag.ExitOnError),
		cmdUsage: "Get the remote file(src) and download to local file(des).",
	}
	getCmd.String("src", "", "(required) the remote file on chunk server.")
	getCmd.String("des", "./out.txt", "(required) the local file.")

	addCmd := &Flags{
		FlagSet:  flag.NewFlagSet("add", flag.ExitOnError),
		cmdUsage: "Put the local file(src) and upload to remote file(des).",
	}
	addCmd.String("src", "", "(required) the remote file on chunk server.")
	addCmd.String("des", "", "(required) the local file.")

	removeCmd := &Flags{
		FlagSet:  flag.NewFlagSet("remove", flag.ExitOnError),
		cmdUsage: "Remove the remote file(des).",
	}
	removeCmd.String("des", "", "(required) the remote file.")

	moveCmd := &Flags{
		FlagSet:  flag.NewFlagSet("move", flag.ExitOnError),
		cmdUsage: "Move the remote file(src) to another remote file(des).",
	}
	moveCmd.String("src", "", "(required) the remote file on chunk server.")
	moveCmd.String("des", "", "(required) the remote file that src moved to.")

	listCmd := &Flags{
		FlagSet:  flag.NewFlagSet("list", flag.ExitOnError),
		cmdUsage: "List the all files in the remote Directory(des).",
	}
	listCmd.String("des", "", "(required) the remote Directory.")

	// 注册
	subcommands := map[string]*Flags{
		getCmd.Name():    getCmd,
		addCmd.Name():    addCmd,
		removeCmd.Name(): removeCmd,
		moveCmd.Name():   moveCmd,
		listCmd.Name():   listCmd,
	}

	if len(os.Args) < 2 {
		showUsage(subcommands)
	}

	cmd = subcommands[os.Args[1]]
	if cmd == nil {
		showUsage(subcommands)
	}

	err := cmd.Parse(os.Args[2:])
	if err != nil {
		showUsage(subcommands)
	}

}

func showUsage(subcommands map[string]*Flags) {
	fmt.Printf("Usage: .\\cmd.exe COMMAND\n\n")
	for _, v := range subcommands {
		fmt.Printf("%s %s\n", v.Name(), v.cmdUsage)
		v.PrintDefaults()
		fmt.Println()
	}
	os.Exit(2)
}
